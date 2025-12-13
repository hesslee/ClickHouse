#!/bin/bash
set -e

CLICKHOUSE_SERVER=clickhouse-server
CLICKHOUSE_CLIENT=clickhouse-client
CONFIG_FILE="tests/shared/jfs/config.xml"

# Start Server
echo "Starting ClickHouse Server..."
$CLICKHOUSE_SERVER --config-file=$CONFIG_FILE --daemon

# Wait for server to start
echo "Waiting for server to start..."
sleep 15

# Create Database and Table
echo "Creating Database and Table..."
$CLICKHOUSE_CLIENT --port 9000 --query "CREATE DATABASE IF NOT EXISTS shared_db ENGINE = Shared('/clickhouse/databases/shared_db', 'replica_1');"
$CLICKHOUSE_CLIENT --port 9000 --query "CREATE TABLE IF NOT EXISTS shared_db.test_table (id UInt64, data String) ENGINE = SharedMergeTree ORDER BY id SETTINGS storage_policy = 'jfs_policy';"

# Insert Data
echo "Inserting Data..."
$CLICKHOUSE_CLIENT --port 9000 --query "INSERT INTO shared_db.test_table VALUES (1, 'test1'), (2, 'test2');"

# Verify Data
echo "Verifying Data..."
RESULT=$($CLICKHOUSE_CLIENT --port 9000 --query "SELECT * FROM shared_db.test_table ORDER BY id;")
echo "Result: $RESULT"

if [[ "$RESULT" == *"1	test1"* ]] && [[ "$RESULT" == *"2	test2"* ]]; then
    echo "Test PASSED"
else
    echo "Test FAILED"
    # Dump detailed info if failed
    $CLICKHOUSE_CLIENT --port 9000 --query "SELECT * FROM shared_db.test_table;"
fi

# Stop Server
echo "Stopping ClickHouse Server..."
pkill -F ./tmp/clickhouse-server.pid || true
