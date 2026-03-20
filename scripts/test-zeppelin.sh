#!/bin/bash

# Zeppelin + Spark Test Script
# This script performs basic health checks on the Zeppelin stack

echo "🔍 Zeppelin + Spark Health Check"
echo "=================================="
echo ""

# Check if containers are running
echo "📦 Checking containers..."
echo ""

containers=("zeppelin-custom" "spark-master" "spark-worker" "namenode")
all_running=true

for container in "${containers[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        status=$(docker ps --filter "name=${container}" --format "{{.Status}}")
        echo "✅ $container: $status"
    else
        echo "❌ $container: NOT RUNNING"
        all_running=false
    fi
done

echo ""

if [ "$all_running" = false ]; then
    echo "⚠️  Some containers are not running. Start them with:"
    echo "   docker-compose --profile zeppelin-custom up -d"
    exit 1
fi

# Check Zeppelin health
echo "🌐 Checking Zeppelin Web UI..."
if curl -sf http://localhost:8080 > /dev/null; then
    echo "✅ Zeppelin is accessible at http://localhost:8080"
else
    echo "❌ Zeppelin is not accessible at http://localhost:8080"
fi

echo ""

# Check Spark Master UI
echo "🌐 Checking Spark Master UI..."
if curl -sf http://localhost:8082 > /dev/null; then
    echo "✅ Spark Master is accessible at http://localhost:8082"
else
    echo "❌ Spark Master is not accessible at http://localhost:8082"
fi

echo ""

# Check Spark Worker UI
echo "🌐 Checking Spark Worker UI..."
if curl -sf http://localhost:8083 > /dev/null; then
    echo "✅ Spark Worker is accessible at http://localhost:8083"
else
    echo "❌ Spark Worker is not accessible at http://localhost:8083"
fi

echo ""

# Check Python in Zeppelin
echo "🐍 Checking Python installation in Zeppelin..."
python_version=$(docker exec zeppelin-custom python3 --version 2>&1)
if [ $? -eq 0 ]; then
    echo "✅ Python installed: $python_version"
else
    echo "❌ Python not found in Zeppelin container"
fi

echo ""

# Check Python in Spark Worker
echo "🐍 Checking Python in Spark Worker..."
worker_python=$(docker exec spark-worker python3 --version 2>&1)
if [ $? -eq 0 ]; then
    echo "✅ Python installed in worker: $worker_python"
else
    echo "❌ Python not found in Spark Worker"
fi

echo ""

# Check CSV files
echo "📁 Checking CSV files..."
csv_count=$(docker exec zeppelin-custom ls -1 /opt/zeppelin/csv 2>/dev/null | wc -l)
echo "✅ Found $csv_count files in /opt/zeppelin/csv"
docker exec zeppelin-custom ls -lh /opt/zeppelin/csv 2>/dev/null | head -10

echo ""

# Check HDFS
echo "🗄️  Checking HDFS..."
if docker exec namenode hdfs dfs -ls / > /dev/null 2>&1; then
    echo "✅ HDFS is accessible"
else
    echo "❌ HDFS is not accessible"
fi

echo ""
echo "=================================="
echo "✨ Health check complete!"
echo ""
echo "📚 Next steps:"
echo "   1. Open Zeppelin: http://localhost:8080"
echo "   2. Follow the validation guide: ZEPPELIN_VALIDATION.md"
echo "   3. Check Spark UI: http://localhost:8082"
echo ""
