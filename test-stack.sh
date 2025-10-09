#!/bin/bash
# Don't exit on errors - we want to continue testing even if some tests fail
# set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

# Helper functions
print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_test() {
    echo -e "${YELLOW}[TEST]${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
    ((TESTS_PASSED++))
}

print_error() {
    echo -e "${RED}✗${NC} $1"
    ((TESTS_FAILED++))
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# Test function wrapper
run_test() {
    local test_name="$1"
    local test_command="$2"

    print_test "$test_name"
    if eval "$test_command" > /dev/null 2>&1; then
        print_success "$test_name passed"
        return 0
    else
        print_error "$test_name failed"
        return 1
    fi
}

# Main test script
echo -e "${GREEN}╔════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║  Hadoop Big Data Stack - Test Suite       ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════╝${NC}"

# Check if Docker Compose is running
print_header "1. Checking Docker Services"

services=("namenode" "datanode1" "datanode2" "resourcemanager" "nodemanager" "hive-postgres" "hive-metastore" "hive-hs2")
for service in "${services[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${service}$"; then
        print_success "$service is running"
    else
        print_error "$service is not running"
    fi
done

# Wait for services to be ready
print_header "2. Waiting for Services to be Ready"
print_info "Waiting 10 seconds for services to stabilize..."
sleep 10

# Test HDFS
print_header "3. Testing HDFS"

print_test "List HDFS root directory"
if docker exec namenode hdfs dfs -ls / 2>&1; then
    print_success "HDFS root listing successful"
else
    print_error "HDFS root listing failed"
fi

print_test "Create test directory in HDFS"
if docker exec namenode hdfs dfs -mkdir -p /test/data 2>&1; then
    print_success "Directory creation successful"
else
    print_error "Directory creation failed"
fi

print_test "Upload test file to HDFS"
if docker exec namenode bash -c "echo 'Hello HDFS - Test File' | hdfs dfs -put - /test/data/test.txt" 2>&1; then
    print_success "File upload successful"
else
    print_error "File upload failed"
fi

print_test "Read test file from HDFS"
content=$(docker exec namenode hdfs dfs -cat /test/data/test.txt 2>&1)
if echo "$content" | grep -q "Hello HDFS"; then
    print_success "File read successful: $content"
else
    print_error "File read failed"
fi

print_test "Check HDFS replication"
if docker exec namenode hdfs dfs -stat '%r' /test/data/test.txt | grep -q "2"; then
    print_success "Replication factor is correct (2)"
else
    print_error "Replication factor is incorrect"
fi

print_test "Verify DataNodes are connected"
datanode_count=$(docker exec namenode hdfs dfsadmin -report 2>&1 | grep "Live datanodes" | grep -o '[0-9]\+' | head -1)
if [ "$datanode_count" = "2" ]; then
    print_success "Both DataNodes are connected ($datanode_count/2)"
else
    print_error "DataNode count is incorrect ($datanode_count/2)"
fi

# Test YARN
print_header "4. Testing YARN"

print_test "Check YARN ResourceManager"
if curl -sf http://localhost:8088 > /dev/null; then
    print_success "ResourceManager web UI is accessible"
else
    print_error "ResourceManager web UI is not accessible"
fi

print_test "List YARN nodes"
node_count=$(docker exec resourcemanager yarn node -list 2>&1 | grep -c "RUNNING" || echo "0")
if [ "$node_count" -ge "1" ]; then
    print_success "NodeManager is registered and running"
else
    print_error "No NodeManagers found"
fi

# Test PostgreSQL
print_header "5. Testing PostgreSQL (Hive Metastore DB)"

print_test "Connect to PostgreSQL"
if docker exec hive-postgres psql -U hive -d metastore_db -c '\dt' > /dev/null 2>&1; then
    print_success "PostgreSQL connection successful"
else
    print_error "PostgreSQL connection failed"
fi

# Test Hive
print_header "6. Testing Hive"

print_test "Check Hive Metastore port"
if docker exec hive-metastore bash -c "timeout 2 bash -c '</dev/tcp/localhost/9083' 2>/dev/null"; then
    print_success "Hive Metastore is listening on port 9083"
else
    print_error "Hive Metastore is not accessible"
fi

print_test "Check HiveServer2 port"
if docker exec hive-hs2 bash -c "timeout 2 bash -c '</dev/tcp/localhost/10000' 2>/dev/null"; then
    print_success "HiveServer2 is listening on port 10000"
else
    print_error "HiveServer2 is not accessible"
fi

print_test "Create Hive database"
if docker exec hive-hs2 beeline -u 'jdbc:hive2://localhost:10000/' -e "CREATE DATABASE IF NOT EXISTS test_db;" 2>&1 | grep -qE "(OK|No rows affected)"; then
    print_success "Hive database creation successful"
else
    print_error "Hive database creation failed"
fi

print_test "Create Hive table"
if docker exec hive-hs2 beeline -u 'jdbc:hive2://localhost:10000/' -e "
USE test_db;
CREATE TABLE IF NOT EXISTS test_table (
    id INT,
    name STRING,
    value DOUBLE
) STORED AS PARQUET;
" 2>&1 | grep -qE "(OK|No rows affected)"; then
    print_success "Hive table creation successful"
else
    print_error "Hive table creation failed"
fi

print_test "Insert data into Hive table"
if docker exec hive-hs2 beeline -u 'jdbc:hive2://localhost:10000/' -e "
USE test_db;
INSERT INTO test_table VALUES
    (1, 'test1', 100.5),
    (2, 'test2', 200.75),
    (3, 'test3', 300.25);
" 2>&1 | grep -qE "(OK|rows affected)"; then
    print_success "Hive data insertion successful"
else
    print_error "Hive data insertion failed"
fi

print_test "Query Hive table"
result=$(docker exec hive-hs2 beeline -u 'jdbc:hive2://localhost:10000/' --outputformat=csv2 -e "USE test_db; SELECT COUNT(*) as cnt FROM test_table;" 2>&1)
if echo "$result" | grep -q "3"; then
    print_success "Hive query successful (found 3 records)"
else
    print_error "Hive query failed or returned unexpected results"
fi

# Test Web UIs (Optional - based on profile selection)
print_header "7. Testing Web UI Services"

print_test "Check Zeppelin web interface"
if docker ps --format '{{.Names}}' | grep -q "^zeppelin$"; then
    if curl -sf http://localhost:8080 > /dev/null; then
        print_success "Zeppelin web interface is accessible at http://localhost:8080"
    else
        print_error "Zeppelin container is running but web interface is not accessible"
    fi
else
    print_info "Zeppelin is not running (use --profile zeppelin or --profile all to start)"
fi

print_test "Check Hue web interface"
if docker ps --format '{{.Names}}' | grep -q "^hue$"; then
    if curl -sf http://localhost:8888 > /dev/null; then
        print_success "Hue web interface is accessible at http://localhost:8888"
    else
        print_error "Hue container is running but web interface is not accessible"
    fi
else
    print_info "Hue is not running (use --profile hue or --profile all to start)"
fi

# Test WebHDFS
print_header "8. Testing WebHDFS Integration"

print_test "Test WebHDFS API"
if curl -sf "http://localhost:9870/webhdfs/v1/?op=LISTSTATUS" | grep -q "FileStatuses"; then
    print_success "WebHDFS API is working"
else
    print_error "WebHDFS API is not accessible"
fi

print_test "List test directory via WebHDFS"
if curl -sf "http://localhost:9870/webhdfs/v1/test/data?op=LISTSTATUS" | grep -q "test.txt"; then
    print_success "WebHDFS can access test files"
else
    print_error "WebHDFS cannot access test files"
fi

# Cleanup
print_header "9. Cleanup"

print_test "Remove test data from HDFS"
if docker exec namenode hdfs dfs -rm -r /test 2>&1; then
    print_success "Test data cleaned up from HDFS"
else
    print_error "Failed to clean up test data from HDFS"
fi

print_test "Drop test Hive database"
if docker exec hive-hs2 beeline -u 'jdbc:hive2://localhost:10000/' -e "DROP DATABASE IF EXISTS test_db CASCADE;" 2>&1 | grep -qE "(OK|No rows affected)"; then
    print_success "Test database cleaned up from Hive"
else
    print_error "Failed to clean up test database from Hive"
fi

# Final report
print_header "Test Summary"
total_tests=$((TESTS_PASSED + TESTS_FAILED))
echo -e "Total tests: ${BLUE}$total_tests${NC}"
echo -e "Passed: ${GREEN}$TESTS_PASSED${NC}"
echo -e "Failed: ${RED}$TESTS_FAILED${NC}"

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "\n${GREEN}╔════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  ✓ All tests passed successfully!         ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════╝${NC}\n"
    exit 0
else
    echo -e "\n${RED}╔════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║  ✗ Some tests failed. Check logs above.   ║${NC}"
    echo -e "${RED}╚════════════════════════════════════════════╝${NC}\n"
    exit 1
fi
