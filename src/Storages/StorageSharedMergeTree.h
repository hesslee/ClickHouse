#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

class BackgroundJobsAssignee; // Forward declaration for BackgroundJobsAssignee

class StorageSharedMergeTree final : public MergeTreeData
{
public:
    StorageSharedMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        std::unique_ptr<MergeTreeSettings> settings_);

    std::string getName() const override { return "Shared" + merging_params.getModeName() + "MergeTree"; }

    bool supportsParallelInsert() const override { return true; }
    bool supportsReplication() const override { return true; } // It is "replicated" via shared storage
    bool supportsDeduplication() const override { return true; }

    void startup() override;
    void shutdown(bool is_drop) override;

    // Write data to shared storage and commit to Keeper
    SinkToStoragePtr
    write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_, bool async_insert) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    MutationCounters getMutationCounters() const override;
    std::map<std::string, MutationCommands> getUnfinishedMutationCommands() const override;
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override;
    MutationsSnapshotPtr getMutationsSnapshot(const IMutationsSnapshot::Params & params) const override;
    void dropPartNoWaitNoThrow(const String & part_name) override;
    void dropPart(const String & part_name, bool detach, ContextPtr context_) override;
    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr context_) override;
    PartitionCommandsResultInfo
    attachPartition(const ASTPtr & partition, const StorageMetadataPtr & metadata_snapshot, bool part, ContextPtr context_) override;
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context_) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context_) override;
    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;
    void attachRestoredParts(MutableDataPartsVector && parts) override;
    void startBackgroundMovesIfNeeded() override;
    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;

    std::pair<size_t, size_t> getMaxPartsCountAndSizeForPartition() const override;
    size_t getMaxOutdatedPartsCountForPartition() const override;

    // Leaderless merge selection
    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr local_context) override;

    Int64 allocateBlockNumber();
    String getZooKeeperPath() const { return zookeeper_path; }

    BackgroundSchedulePool::TaskHolder part_watcher_task;
    void processPartChanges();
    void startPartWatcher();

private:
    String zookeeper_path;
};

}
