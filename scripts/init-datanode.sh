#!/bin/bash
set -euo pipefail

# Make sure datadir exists with proper perms
mkdir -p /opt/hadoop/data/dataNode
chown -R hadoop:hadoop /opt/hadoop/data/dataNode
chmod 755 /opt/hadoop/data/dataNode

# Start DataNode in foreground
echo "Starting DataNode..."
hdfs datanode
