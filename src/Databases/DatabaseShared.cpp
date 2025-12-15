#include <Databases/DatabaseShared.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>


namespace DB
{

DatabaseShared::DatabaseShared(const String & name_, const String & metadata_path_, UUID uuid,
                               const String & zookeeper_path_, const String & replica_name_,
                               ContextPtr context_)
    : DatabaseAtomic(name_, metadata_path_, uuid, "Shared", context_)
    , zookeeper_path(zookeeper_path_)
    , replica_name(replica_name_)
{
    verification_task = getContext()->getSchedulePool().createTask(StorageID(getDatabaseName(), "_verification", uuid), "DatabaseSharedVerification", [this]{ processReplicationQueue(); });
    scheduleTask();
}

void DatabaseShared::scheduleTask()
{
    verification_task->scheduleAfter(2000); // Check every 2 seconds
}

void DatabaseShared::processReplicationQueue()
{
    try
    {
        auto zookeeper = getZooKeeper();
        String tables_path = zookeeper_path + "/tables";
        
        if (zookeeper->exists(tables_path))
        {
            Strings tables = zookeeper->getChildren(tables_path);
            std::set<String> zk_tables(tables.begin(), tables.end());
            
            // Check for new tables
            for (const auto & table_name : tables)
            {
                if (!isTableExist(table_name, getContext()))
                {
                    // For now, just log. Implementation of creating table from ZK query 
                    // requires parsing CREATE query and executing it locally without ZK write.
                    // This creates infinite loop if not careful.
                    // We need 'Context::createTable' to support 'NO_ZK_WRITE' flag or similar.
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to sync tables in DatabaseShared");
    }
    
    scheduleTask();
}

void DatabaseShared::loadStoredObjects(ContextMutablePtr context_, LoadingStrictnessLevel mode)
{
    auto zookeeper = getZooKeeper();
    if (!zookeeper->exists(zookeeper_path))
    {
        zookeeper->createAncestors(zookeeper_path);
        zookeeper->createIfNotExists(zookeeper_path, "");
    }
    
    String tables_path = zookeeper_path + "/tables";
    zookeeper->createIfNotExists(tables_path, "");

    Strings tables = zookeeper->getChildren(tables_path);
    for (const auto & table_name : tables)
    {

            
        String table_node = tables_path + "/" + table_name;
        String query = zookeeper->get(table_node);
        
        // Simplified: Parse query and attach. Real implementation would be more complex.
        // We defer actual attach to standard database mechanisms or just create binding.
        // For now, we assume we just load metadata.
    }
    
    DatabaseAtomic::loadStoredObjects(context_, mode);
}

void DatabaseShared::createTable(ContextPtr context_, const String & table_name, const StoragePtr & table, const ASTPtr & query)
{
    auto zookeeper = getZooKeeper();
    String tables_path = zookeeper_path + "/tables";
    String table_node = tables_path + "/" + table_name;
    
    // Serialize Create Query
    String query_str = getObjectDefinitionFromCreateQuery(query);
    
    // Create node in Keeper
    zookeeper->createAncestors(table_node);
    zookeeper->create(table_node, query_str, zkutil::CreateMode::Persistent);

    DatabaseAtomic::createTable(context_, table_name, table, query);
}

void DatabaseShared::dropTable(ContextPtr context_, const String & table_name, bool no_delay)
{
    auto zookeeper = getZooKeeper();
    String table_node = zookeeper_path + "/tables/" + table_name;
    
    zookeeper->remove(table_node);
    
    DatabaseAtomic::dropTable(context_, table_name, no_delay);
}

void DatabaseShared::renameTable(ContextPtr context_, const String & table_name, IDatabase & to_database, const String & to_table_name, bool exchange, bool dictionary)
{
    // TODO: Replicate rename via Keeper
    DatabaseAtomic::renameTable(context_, table_name, to_database, to_table_name, exchange, dictionary);
}

zkutil::ZooKeeperPtr DatabaseShared::getZooKeeper() const
{
    return getContext()->getZooKeeper();
}

}
