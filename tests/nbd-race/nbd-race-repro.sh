#!/bin/bash
# Reproducer for the nbd plugin close/open lifecycle race (see
# connectionLifecycleMutex in nbd/viperblock.go). Requires: a predastore
# dev cluster, an initialised volume (createvol-s3), nbdkit + the plugin.
# Usage: nbd-race-repro.sh <socket-path> <n-iterations>
# Each iteration opens a fresh NBD connection (qemu-img info) back to back;
# pre-fix this hits "failed to save block state ... checkpoints" within a
# few dozen iterations. Post-fix: zero failures expected.
set -u
SOCK=$1; N=${2:-40}; FAILS=0
for i in $(seq "$N"); do
  OUT=$(qemu-img info "nbd+unix:///?socket=$SOCK" 2>&1) || true
  if echo "$OUT" | grep -q "Could not recover local WALs"; then
    FAILS=$((FAILS+1)); echo "iter $i: RACE HIT"
  fi
done
echo "iterations=$N race_hits=$FAILS"
test "$FAILS" -eq 0
