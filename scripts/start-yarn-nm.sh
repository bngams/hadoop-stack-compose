#!/bin/bash
set -euo pipefail

# Create local directories for NodeManager
mkdir -p /tmp/nm-local-dir /tmp/nm-log-dir
chown -R hadoop:hadoop /tmp/nm-local-dir /tmp/nm-log-dir

echo "Starting YARN NodeManager..."
yarn nodemanager
