#!/usr/bin/env bash
# Plays the rolling-deploy story from cadence-workflow/cadence#8783 against a local Cadence server,
# once with the code from master (before) and once with the fix (after).
set -uo pipefail

DEMO_SRC=$(mktemp -d)
cp -r versiondemo "$DEMO_SRC/"

build() { # build <git ref> <output binary>
  git checkout -q "$1"
  rm -rf versiondemo && cp -r "$DEMO_SRC/versiondemo" .
  go build -o "$2" ./versiondemo
  rm -rf versiondemo
}

scenario() { # scenario <binary> <label>
  local bin=$1 label=$2 out="logs/$2"
  mkdir -p "$out"
  "$bin" register

  echo "=== [$label] new worker (supports version 2) starts the workflow"
  "$bin" worker -max 2 >"$out/1-new-worker.log" 2>&1 & pid=$!
  sleep 10
  "$bin" start
  sleep 10
  kill -INT $pid; wait $pid

  echo "=== [$label] rolled back: only the old worker (supports version 1) is running"
  "$bin" worker -max 1 >"$out/2-old-worker.log" 2>&1 & pid=$!
  sleep 5
  "$bin" signal
  sleep 45
  kill -INT $pid; wait $pid

  echo "=== [$label] new worker is back"
  "$bin" worker -max 2 >"$out/3-new-worker-again.log" 2>&1 & pid=$!
  sleep 30
  kill -INT $pid; wait $pid

  for f in "$out"/*.log; do
    echo
    echo "################ [$label] $(basename "$f") ################"
    grep -E "Workflow panic|ProcessEvent panic|Workflow version mismatch|Workflow is using version|Workflow received the signal|>>>|decision-task|no panic" "$f" \
      | sed -E 's/"PanicStack": "[^"]*"//' | uniq
  done
}

build origin/master /tmp/demo-before
build "$GITHUB_SHA" /tmp/demo-after

scenario /tmp/demo-before before
scenario /tmp/demo-after after
