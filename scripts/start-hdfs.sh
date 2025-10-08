#!/bin/bash
set -euo pipefail

# Format only once
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
  echo "Formatting NameNode..."
  hdfs namenode -format -force -nonInteractive
fi

# Start NameNode in foreground
echo "Starting NameNode..."
hdfs namenode
