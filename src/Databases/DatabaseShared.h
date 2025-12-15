#pragma once

#include <Databases/DatabaseAtomic.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{



class DatabaseShared final : public DatabaseAtomic
{
public:
    DatabaseShared(const String & name_, const String & metadata_path_, UUID uuid,
                   const String & zookeeper_path_, const String & replica_name_,
                   ContextPtr context_);

    std::string getEngineName() const override { return "Shared"; }

    void loadStoredObjects(ContextMutablePtr context_, LoadingStrictnessLevel mode) override;

    void createTable(ContextPtr context_, const String & table_name, const StoragePtr & table, const ASTPtr & query) override;
    void dropTable(ContextPtr context_, const String & table_name, bool no_delay) override;
    void renameTable(ContextPtr context_, const String & table_name, IDatabase & to_database, const String & to_table_name, bool exchange, bool dictionary) override;

private:
    String zookeeper_path;
    String replica_name;
    
    zkutil::ZooKeeperPtr getZooKeeper() const;

    BackgroundSchedulePool::TaskHolder verification_task;
    void processReplicationQueue();
    void scheduleTask();
};

}
