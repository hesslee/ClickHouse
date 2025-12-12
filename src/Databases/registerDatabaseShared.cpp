#include <Databases/DatabaseShared.h>
#include <Databases/DatabaseFactory.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

void registerDatabaseShared(DatabaseFactory & factory)
{
    auto callback = [](const DatabaseFactory::Arguments & arguments)
    {
        // Engine definition: Shared('/path/to/db', 'replica_name')
        // Or simplified: Shared() which defaults to /clickhouse/databases/{db_name}

        String zookeeper_path;
        String replica_name;

        if (arguments.engine_args.empty())
        {
            zookeeper_path = "/clickhouse/databases/" + arguments.database_name;
            replica_name = "replica1"; // Simple default or generate UUID
        }
        else if (arguments.engine_args.size() == 2)
        {
            zookeeper_path = arguments.engine_args[0]->as<ASTLiteral>()->value.safeGet<String>();
            replica_name = arguments.engine_args[1]->as<ASTLiteral>()->value.safeGet<String>();
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shared database requires 0 or 2 arguments: path and replica name");
        }

        return std::make_shared<DatabaseShared>(
            arguments.database_name, arguments.metadata_path, arguments.uuid, zookeeper_path, replica_name, arguments.context);
    };

    factory.registerDatabase("Shared", callback, DatabaseFactory::EngineFeatures{ .supports_arguments = true });
}

}
