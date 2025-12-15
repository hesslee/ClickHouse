#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/Compaction/MergeSelectorApplier.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/PartsCollectors/IPartsCollector.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageSharedMergeTree.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

namespace MergeTreeSetting
{
extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
}

StorageSharedMergeTree::StorageSharedMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata_,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> settings_)
    : MergeTreeData(
          table_id_,
          metadata_,
          context_,
          date_column_name,
          merging_params_,
          std::move(settings_),
          false,
          LoadingStrictnessLevel::ATTACH,
          {})
{
    // Initialize directories and format version using the relative_data_path
    initializeDirectoriesAndFormatVersion(relative_data_path_, true /* attach */, date_column_name, false /* need_create_directories */);
}

void StorageSharedMergeTree::startup()
{
    // Initialize ZK path
    zookeeper_path = "/clickhouse/tables/" + toString(getStorageID().uuid);

    auto zookeeper = getContext()->getZooKeeper();
    if (!zookeeper->exists(zookeeper_path))
    {
        zookeeper->createAncestors(zookeeper_path);
        zookeeper->createIfNotExists(zookeeper_path, "");
        zookeeper->createIfNotExists(zookeeper_path + "/columns", "");
        zookeeper->createIfNotExists(zookeeper_path + "/parts", "");
    }
    zookeeper->createIfNotExists(zookeeper_path + "/blocks", "");

    MergeTreeData::startup();
    startPartWatcher();
}

void StorageSharedMergeTree::shutdown(bool is_drop)
{
    MergeTreeData::shutdown(is_drop);
}


class SharedMergeTreeSink : public SinkToStorage
{
public:
    SharedMergeTreeSink(StorageSharedMergeTree & storage_, StorageMetadataPtr metadata_snapshot_, ContextPtr context_)
        : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , context(context_)
    {
    }

    String getName() const override { return "SharedMergeTreeSink"; }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());

        // 1. Split block into parts
        // Use default max_parts_per_block = 0 (unlimited/default)
        auto part_blocks = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), 0, metadata_snapshot, context);

        MergeTreeDataWriter writer(storage);

        for (auto & current_block : part_blocks)
        {
            // 2. Write temp part
            auto temp_part = writer.writeTempPart(current_block, metadata_snapshot, context);

            // 3. Commit part (rename temp to actual part)
            if (temp_part->part)
            {
                temp_part->finalize();

                MergeTreeData::Transaction transaction(storage, context->getCurrentTransaction().get());
                auto lock = storage.lockParts();

                // Allocate block number from ZooKeeper
                Int64 block_number = storage.allocateBlockNumber();
                temp_part->part->info.min_block = block_number;
                temp_part->part->info.max_block = block_number;
                temp_part->part->info.level = 0;
                temp_part->part->info.mutation = 0;

                temp_part->part->setName(temp_part->part->info.getPartNameAndCheckFormat(storage.format_version));

                // Rename and add
                storage.renameTempPartAndAdd(temp_part->part, transaction, lock, false);

                transaction.commit(lock);

                // 4. Publish to ZooKeeper
                auto zookeeper = storage.getContext()->getZooKeeper();
                String part_node = storage.getZooKeeperPath() + "/parts/" + temp_part->part->name;

                // Serialize part state
                WriteBufferFromOwnString wb;

                writeIntText(temp_part->part->rows_count, wb);
                writeChar('\n', wb);
                writeIntText(temp_part->part->getBytesOnDisk(), wb);
                writeChar('\n', wb);

                temp_part->part->checksums.write(wb);
                writeChar('\n', wb);
                temp_part->part->getColumns().writeText(wb);
                writeChar('\n', wb);

                zookeeper->create(part_node, wb.str(), zkutil::CreateMode::Persistent);
            }
        }
    }

private:
    StorageSharedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
};

SinkToStoragePtr StorageSharedMergeTree::write(
    const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, bool /*async_insert*/)
{
    // Return our custom sink that writes to local disk (via MergeTreeDataWriter)
    // In the future this will be replaced with SharedMergeTreeSink that writes to S3/Keeper
    return std::make_shared<SharedMergeTreeSink>(*this, metadata_snapshot, context_);
}

void StorageSharedMergeTree::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    // Minimal implementation delegating to MergeTreeDataSelectExecutor
    MergeTreeDataSelectExecutor reader(*this);

    auto plan = reader.read(column_names, storage_snapshot, query_info, context_, max_block_size, num_streams);

    if (plan)
        query_plan = std::move(*plan);
}

// Minimalistic Parts Collector for SharedMergeTree
class SharedPartsCollector : public IPartsCollector
{
public:
    SharedPartsCollector(MergeTreeData & data_)
        : data(data_)
    {
    }

    PartsRanges grabAllPossibleRanges(
        const StorageMetadataPtr & metadata_snapshot,
        const StoragePolicyPtr & storage_policy,
        const time_t & current_time,
        const std::optional<PartitionIdsHint> & /*partitions_hint*/,
        LogSeriesLimiter & /*series_log*/) const override
    {
        auto log = getLogger("SharedMergeTree");

        // Simply return all active parts grouped by partition
        PartsRanges ranges;
        auto parts = data.getDataPartsVectorForInternalUsage();

        LOG_DEBUG(
            log, "SharedPartsCollector::grabAllPossibleRanges: Found {} parts from getDataPartsVectorForInternalUsage()", parts.size());

        std::map<String, std::vector<MergeTreeData::DataPartPtr>> parts_by_partition;
        for (const auto & part : parts)
        {
            LOG_DEBUG(log, "  Part: {} state={}", part->name, static_cast<int>(part->getState()));
            parts_by_partition[part->info.getPartitionId()].push_back(part);
        }

        for (const auto & [partition_id, partition_parts] : parts_by_partition)
        {
            PartsRange range;
            for (const auto & part : partition_parts)
                range.push_back(buildPartProperties(part, metadata_snapshot, storage_policy, current_time));
            if (!range.empty())
                ranges.push_back(std::move(range));
        }

        LOG_DEBUG(log, "SharedPartsCollector::grabAllPossibleRanges: Returning {} ranges", ranges.size());
        return ranges;
    }

    std::expected<PartsRange, PreformattedMessage> grabAllPartsInsidePartition(
        const StorageMetadataPtr & /*metadata_snapshot*/,
        const StoragePolicyPtr & /*storage_policy*/,
        const time_t & /*current_time*/,
        const std::string & /*partition_id*/) const override
    {
        // TODO: implement for partition
        return {};
    }

private:
    MergeTreeData & data;
};

// Minimalistic Merge Predicate
class SharedMergePredicate : public IMergePredicate
{
public:
    std::expected<void, PreformattedMessage> canMergeParts(const PartProperties & /*left*/, const PartProperties & /*right*/) const override
    {
        // Allow all merges for now
        return {};
    }

    PartsRange getPatchesToApplyOnMerge(const PartsRange & /*range*/) const override { return {}; }
};

bool StorageSharedMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & /*assignee*/)
{
    auto log = getLogger("SharedMergeTree");

    // 1. Initialize MergerMutator
    MergeTreeDataMergerMutator mutator(*this);

    // 2. Select parts to merge
    auto parts_collector = std::make_shared<SharedPartsCollector>(*this);
    auto merge_predicate = std::make_shared<SharedMergePredicate>();

    // Use MergeSelectorApplier directly
    MergeSelectorApplier selector(
        {100ULL * 1024 * 1024 * 1024}, // max_merge_sizes, 100GB as placeholder
        false, // ttl_allowed
        false, // aggressive
        nullptr // range_filter
    );

    LOG_DEBUG(log, "StorageSharedMergeTree::scheduleDataProcessingJob: Selecting parts to merge");

    auto result = mutator.selectPartsToMerge(parts_collector, merge_predicate, selector, std::nullopt);

    if (!result)
    {
        LOG_DEBUG(
            log,
            "StorageSharedMergeTree::scheduleDataProcessingJob: No parts selected for merge. Reason: {}",
            result.error().explanation.text);
        return false;
    }

    LOG_DEBUG(log, "StorageSharedMergeTree::scheduleDataProcessingJob: Selected {} merge choices", result.value().size());

    // 3. Process selected merges
    for (const auto & choice : result.value())
    {
        // 4. Try to lock in ZooKeeper (Simplified Leaderless Coordination)

        Strings part_names;
        for (const auto & part_prop : choice.range)
            part_names.push_back(part_prop.name);

        String merge_id = "merge_" + toString(CityHash_v1_0_2::CityHash64(fmt::format("{}", fmt::join(part_names, ",")).c_str(), 0));
        String lock_path = getZooKeeperPath() + "/merges/" + merge_id;

        auto zookeeper = getContext()->getZooKeeper();
        zookeeper->createIfNotExists(getZooKeeperPath() + "/merges", "");

        // Use a UUID as the data for the lock node
        String lock_data = toString(UUIDHelpers::generateV4());
        Coordination::Error code = zookeeper->tryCreate(lock_path, lock_data, zkutil::CreateMode::Ephemeral);

        if (code == Coordination::Error::ZNODEEXISTS)
            continue; // Already being merged

        if (code != Coordination::Error::ZOK)
            continue;

        // 5. Execute Merge
        try
        {
            // Prepare parts vector - fetch actual DataPartPtr by name
            MergeTreeData::DataPartsVector parts;
            for (const auto & prop : choice.range)
            {
                auto part = getPartIfExists(prop.name, {MergeTreeDataPartState::Active});
                if (!part)
                {
                    zookeeper->remove(lock_path);
                    continue; // Part no longer exists
                }
                parts.push_back(part);
            }

            if (parts.size() < 2)
            {
                zookeeper->remove(lock_path);
                continue; // Not enough parts to merge
            }

            // Create FutureMergedMutatedPart
            auto future_part = std::make_shared<FutureMergedMutatedPart>(parts, MergeTreeData::DataPartsVector{});
            future_part->assign(parts, {});

            // Register in MergeList
            auto table_id = getStorageID();
            auto merge_entry = getContext()->getMergeList().insert(table_id, future_part, getContext());

            // Reserve space (simplified)
            UInt64 total_size = 0;
            for (const auto & p : parts)
                total_size += p->getBytesOnDisk();
            auto reservation_result = getStoragePolicy()->reserve(total_size * 2, 0);

            if (!reservation_result || !reservation_result.value())
            {
                zookeeper->remove(lock_path);
                return false;
            }

            // Convert unique_ptr to shared_ptr for ReservationSharedPtr
            ReservationSharedPtr reservation = std::move(reservation_result.value());

            // Table lock holder
            TableLockHolder table_lock_holder
                = lockForShare(RWLockImpl::NO_QUERY, (*getSettings())[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);

            // Create MergeTask via Mutator
            auto task = mutator.mergePartsToTemporaryPart(
                future_part,
                getInMemoryMetadataPtr(),
                merge_entry.get(),
                nullptr, // projection_merge_list_element
                table_lock_holder,
                time(nullptr), // time_of_merge
                getContext(),
                reservation,
                false, // deduplicate
                {}, // deduplicate_by_columns
                false, // cleanup
                merging_params,
                nullptr // txn
            );

            // Execute the merge task
            while (task->execute())
            {
            }

            // Get the result
            auto new_part = task->getFuture().get();

            if (new_part)
            {
                MergeTreeData::Transaction transaction(*this, nullptr);
                mutator.renameMergedTemporaryPart(new_part, parts, nullptr, transaction);
                auto parts_lock = lockParts();
                transaction.commit(parts_lock);

                // Publish new part to ZK
                // Note: renameMergedTemporaryPart writes to local disk/storage.
                // We need to publish metadata to ZK.

                auto & result_part = new_part; // It's a shared_ptr now?
                String part_node = getZooKeeperPath() + "/parts/" + result_part->name;
                WriteBufferFromOwnString wb;
                writeIntText(result_part->rows_count, wb);
                writeChar('\n', wb);
                writeIntText(result_part->getBytesOnDisk(), wb);
                writeChar('\n', wb);
                result_part->checksums.write(wb);
                writeChar('\n', wb);
                result_part->getColumns().writeText(wb);
                writeChar('\n', wb);
                zookeeper->create(part_node, wb.str(), zkutil::CreateMode::Persistent);

                // Remove source parts from ZK
                for (const auto & part_prop : choice.range)
                {
                    String old_part_node = getZooKeeperPath() + "/parts/" + part_prop.name;
                    zookeeper->remove(old_part_node);
                }
            }
        }
        catch (...)
        {
            zookeeper->remove(lock_path);
            throw;
        }

        // Release lock
        zookeeper->remove(lock_path);

        return true;
    }

    return false;
}

bool StorageSharedMergeTree::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & /*partition*/,
    bool /*final*/,
    bool /*deduplicate*/,
    const Names & /*deduplicate_by_columns*/,
    bool /*cleanup*/,
    ContextPtr /*local_context*/)
{
    // Simplified optimize: just trigger a merge attempt
    BackgroundJobsAssignee dummy_assignee(*this, BackgroundJobsAssignee::Type::DataProcessing, getContext());
    return scheduleDataProcessingJob(dummy_assignee);
}

MutationCounters StorageSharedMergeTree::getMutationCounters() const
{
    return {};
}

std::map<std::string, MutationCommands> StorageSharedMergeTree::getUnfinishedMutationCommands() const
{
    return {};
}

std::vector<MergeTreeMutationStatus> StorageSharedMergeTree::getMutationsStatus() const
{
    return {};
}


class SharedMutationsSnapshot : public MergeTreeData::MutationsSnapshotBase
{
public:
    using MergeTreeData::MutationsSnapshotBase::MutationsSnapshotBase;

    MutationCommands getOnFlyMutationCommandsForPart(const MergeTreeData::DataPartPtr &) const override { return {}; }

    std::shared_ptr<IMutationsSnapshot> cloneEmpty() const override { return std::make_shared<SharedMutationsSnapshot>(); }
    NameSet getAllUpdatedColumns() const override { return {}; }
};

MergeTreeData::MutationsSnapshotPtr StorageSharedMergeTree::getMutationsSnapshot(const IMutationsSnapshot::Params &) const
{
    // Return empty mutations snapshot
    return std::make_shared<SharedMutationsSnapshot>();
}

void StorageSharedMergeTree::dropPartNoWaitNoThrow(const String &)
{
}

void StorageSharedMergeTree::dropPart(const String &, bool, ContextPtr)
{
}

void StorageSharedMergeTree::dropPartition(const ASTPtr &, bool, ContextPtr)
{
}

PartitionCommandsResultInfo StorageSharedMergeTree::attachPartition(const ASTPtr &, const StorageMetadataPtr &, bool, ContextPtr)
{
    return {};
}

void StorageSharedMergeTree::replacePartitionFrom(const StoragePtr &, const ASTPtr &, bool, ContextPtr)
{
}

void StorageSharedMergeTree::movePartitionToTable(const StoragePtr &, const ASTPtr &, ContextPtr)
{
}

bool StorageSharedMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr &) const
{
    return false;
}

void StorageSharedMergeTree::attachRestoredParts(MutableDataPartsVector &&)
{
}

void StorageSharedMergeTree::startBackgroundMovesIfNeeded()
{
}

std::unique_ptr<MergeTreeSettings> StorageSharedMergeTree::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());
}

std::pair<size_t, size_t> StorageSharedMergeTree::getMaxPartsCountAndSizeForPartition() const
{
    return {0, 0};
}

size_t StorageSharedMergeTree::getMaxOutdatedPartsCountForPartition() const
{
    return 0;
}


Int64 StorageSharedMergeTree::allocateBlockNumber()
{
    auto zookeeper = getContext()->getZooKeeper();
    String block_path = zookeeper_path + "/blocks/block-";

    // Create sequential node
    String created_path = zookeeper->create(block_path, "", zkutil::CreateMode::PersistentSequential);

    // Extract number from end of path
    String number_str = created_path.substr(created_path.find_last_of('-') + 1);
    return parse<Int64>(number_str);
}

void StorageSharedMergeTree::startPartWatcher()
{
    part_watcher_task
        = getContext()->getSchedulePool().createTask(getStorageID(), "SharedMergeTreePartWatcher", [this] { processPartChanges(); });
    part_watcher_task->scheduleAfter(1000);
}

void StorageSharedMergeTree::processPartChanges()
{
    try
    {
        auto zookeeper = getContext()->getZooKeeper();
        if (zookeeper->exists(zookeeper_path + "/parts"))
        {
            Strings parts = zookeeper->getChildren(zookeeper_path + "/parts");
            for (const auto & part_name : parts)
            {
                // Check if we already have this part
                if (getPartIfExists(part_name, {MergeTreeDataPartState::Active}))
                    continue;

                // Fetch metadata from ZK
                String part_node = zookeeper_path + "/parts/" + part_name;
                String metadata_str = zookeeper->get(part_node);

                ReadBufferFromString rb(metadata_str);

                size_t rows_count = 0;
                size_t bytes_on_disk = 0;

                readIntText(rows_count, rb);
                assertChar('\n', rb);
                readIntText(bytes_on_disk, rb);
                assertChar('\n', rb);

                MergeTreeDataPartChecksums checksums;
                checksums.read(rb);
                assertChar('\n', rb);

                NamesAndTypesList columns;
                columns.readText(rb);
                assertChar('\n', rb);

                // Create part wrapper (pointing to shared storage)
                auto volume = getStoragePolicy()->getVolumes()[0];

                // Parse part info from name
                MergeTreePartInfo part_info = MergeTreePartInfo::fromPartName(part_name, format_version);

                auto builder = getDataPartBuilder(part_name, volume, part_name, ReadSettings());
                auto part = builder.withPartInfo(part_info).withPartType(MergeTreeDataPartType::Wide).build();

                part->rows_count = rows_count;
                part->setBytesOnDisk(bytes_on_disk);
                part->checksums = checksums;
                part->setColumns(columns, SerializationInfoByName(columns, SerializationInfo::Settings{}), 0);

                // We don't have part on local disk, but "Wide" part expects files.
                // For SharedMergeTree, we should probably use a "Custom" or "Wide" part
                // but trick it into thinking it's verified.

                // Critical: We must set state to Active and add it.
                // Since this is a hacky "Shared" implementation, we'll try `addPart`.
                // Note: `renameTempPartAndAdd` does validation. `addPart` is protected/private?
                // `startBackgroundMovesIfNeeded` usually does loading.

                // We'll use a transaction to add it safely.
                MergeTreeData::Transaction transaction(*this, nullptr); // No global txn for now
                auto lock = lockParts();

                // If we just add it, it might complain files are missing if we try to read?
                // But for SELECT, we delegate to `MergeTreeDataSelectExecutor`.
                // If that uses `part->volume->getDisk()->readFile`, it will go to S3 (jfs_disk).
                // So as long as `part` points to `jfs_disk`, it should work!

                bool added = renameTempPartAndAdd(part, transaction, lock, false);
                if (added)
                    transaction.commit(lock);

                // Placeholder: Log that we see a new part
                // LOG_TRACE(log, "Saw new part in ZK: {}", part_name);
            }
        }
    }
    catch (...)
    {
        // tryLogCurrentException(log, "Failed to sync parts");
    }
    part_watcher_task->scheduleAfter(2000);
}

}
