#!/usr/bin/env bash
# Example HDFS commands reconstructed from Phase 1 documentation.
# Requires a configured Hadoop installation and a locally available sample input file.
set -euo pipefail

INPUT_FILE="${1:-}"
if [[ -z "$INPUT_FILE" ]]; then
  echo "Usage: bash load_example.sh /path/to/consumption_1_2015_5.txt"
  exit 1
fi

hdfs dfs -mkdir -p /energydata/2015/5
hdfs dfs -put "$INPUT_FILE" /energydata/2015/5/
hdfs dfs -ls /energydata/2015/5/
