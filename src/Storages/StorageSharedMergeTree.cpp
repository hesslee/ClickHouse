#include <Storages/StorageSharedMergeTree.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageFactory.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

StorageSharedMergeTree::StorageSharedMergeTree(
    const StorageID & table_id_,
    const String & /*relative_data_path_*/,
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
        false, // require_part_metadata
        LoadingStrictnessLevel::ATTACH)
{
}

void StorageSharedMergeTree::startup()
{
    // Initialize ZK path
    zookeeper_path = "/clickhouse/tables/" + toString(getStorageID().uuid); // Simplified
    
    auto zookeeper = getContext()->getZooKeeper();
    if (!zookeeper->exists(zookeeper_path))
    {
        zookeeper->createAncestors(zookeeper_path);
        zookeeper->createIfNotExists(zookeeper_path, "");
        zookeeper->createIfNotExists(zookeeper_path + "/columns", "");
        zookeeper->createIfNotExists(zookeeper_path + "/parts", "");
    }
    
    MergeTreeData::startup();
}

void StorageSharedMergeTree::shutdown(bool is_drop)
{
    MergeTreeData::shutdown(is_drop);
}


class SharedMergeTreeSink : public SinkToStorage
{
public:
    SharedMergeTreeSink(
        StorageSharedMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        ContextPtr context_)
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
                
                // Manually fill part name (placeholder logic)
                // In real implementation, we should allocate block number from ZooKeeper
                Int64 block_number = Poco::Timestamp().epochMicroseconds();
                temp_part->part->info.min_block = block_number;
                temp_part->part->info.max_block = block_number;
                temp_part->part->info.level = 0;
                temp_part->part->info.mutation = 0;
                
                temp_part->part->setName(temp_part->part->info.getPartNameAndCheckFormat(storage.format_version));
                
                // Rename and add
                storage.renameTempPartAndAdd(temp_part->part, transaction, lock, false);
                
                transaction.commit(lock);
            }
        }
    }

private:
    StorageSharedMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
};

SinkToStoragePtr StorageSharedMergeTree::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, bool /*async_insert*/)
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
    
    auto plan = reader.read(
        column_names,
        storage_snapshot,
        query_info,
        context_,
        max_block_size,
        num_streams);
    
    if (plan)
        query_plan = std::move(*plan);
}

bool StorageSharedMergeTree::scheduleDataProcessingJob(BackgroundJobsAssignee & /*assignee*/)
{
    // TODO: Implement leaderless merge selection
    // So far we don't have leaderless merge selection, return false to say no job scheduled.
    return false;
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

void StorageSharedMergeTree::dropPartNoWaitNoThrow(const String &) {}

void StorageSharedMergeTree::dropPart(const String &, bool, ContextPtr) {}

void StorageSharedMergeTree::dropPartition(const ASTPtr &, bool, ContextPtr) {}

PartitionCommandsResultInfo StorageSharedMergeTree::attachPartition(const ASTPtr &, const StorageMetadataPtr &, bool, ContextPtr)
{
    return {};
}

void StorageSharedMergeTree::replacePartitionFrom(const StoragePtr &, const ASTPtr &, bool, ContextPtr) {}

void StorageSharedMergeTree::movePartitionToTable(const StoragePtr &, const ASTPtr &, ContextPtr) {}

bool StorageSharedMergeTree::partIsAssignedToBackgroundOperation(const DataPartPtr &) const { return false; }

void StorageSharedMergeTree::attachRestoredParts(MutableDataPartsVector &&) {}

void StorageSharedMergeTree::startBackgroundMovesIfNeeded() {}

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

void StorageSharedMergeTree::loadTableConfig()
{
    // TODO: Load config
}

}
