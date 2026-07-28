#!/bin/bash
# P1.6 latency gate harness (spinifex Phase 1): asserts the human-acked
# bound -- NO single 4 KiB write under sustained saturation exceeds
# 250 ms -- across a repeated-run distribution (default 5 x 30 s
# saturated randwrite legs), measured IN-GUEST through the full
# vhost-user-blk + viperblock-engine (file backend) serving path.
#
# Bound provenance (acked 2026-07-28): repeated-run distributions
# measured maxima of 67 ms (file backend, 5 legs) and 109.3 ms
# (s3/predastore, 6 legs) -- see viperblock/results/2026-07-28_p16_
# repeated_run_distribution.txt and ..._p16_phaseB_s3_production_pair
# .txt. 250 ms gives 2.3-3.7x margin and clears this host's measured
# ambient noise floor (raw-file control spikes to 43 ms).
#
# Requirements: /usr/libexec/qemu-kvm with vhost-user-blk-pci; the
# Debian rig image + ssh key (RIG_IMAGE / RIG_SSH_KEY below); build
# tools for the serve binary (GOTOOLCHAIN=auto GOFIPS140=v1.0.0).
# REPO_DIR must point at a tree containing BOTH the P1.6 engine fixes
# and the vhostuser package: until the Phase-1 integration merge these
# live on fix/backpressure-write-admission-latency and
# feat/data-path-vhost-user-blk respectively -- create a local merge
# worktree and pass REPO_DIR=<merge-tree>. Defaults to this repo root.
# Runtime: ~12-18 min (build + boot + prefill + 5 legs + teardown).
# Prints one verdict line: "=== P16 LATENCY GATE PASS ===" or FAIL.
set -u
REPO_DIR="${REPO_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
RIG_IMAGE="${RIG_IMAGE:-/home/devnull/Downloads/github/vm-images/rig-node1.qcow2}"
RIG_SSH_KEY="${RIG_SSH_KEY:-/home/devnull/Downloads/github/vm-images/rig_ssh_key}"
SSH_PORT="${SSH_PORT:-2239}"
LEGS="${LEGS:-5}"
BOUND_US=250000
SCRATCH="$(mktemp -d /tmp/p16gate.XXXXXX)"

QEMU_PID_FILE="$SCRATCH/qemu.pid"
cleanup() {
  if [ -f "$QEMU_PID_FILE" ] && ps -p "$(cat "$QEMU_PID_FILE")" > /dev/null 2>&1; then
    kill "$(cat "$QEMU_PID_FILE")" 2>/dev/null
    sleep 3
  fi
  pkill -x p16gateserve 2>/dev/null
  # Give the serve's drain+close a moment before removing its tree.
  for _ in $(seq 1 60); do pgrep -x p16gateserve > /dev/null || break; sleep 3; done
  rm -rf "$SCRATCH"
}
trap cleanup EXIT

echo "P16 gate: building serve binary from $REPO_DIR"
(cd "$REPO_DIR" && GOTOOLCHAIN=auto GOFIPS140=v1.0.0 \
  go build -o "$SCRATCH/p16gateserve" ./vhostuser/cmd/vhost-user-blk-serve) || {
  echo "=== P16 LATENCY GATE FAIL === (serve build failed)"; exit 1; }

setsid "$SCRATCH/p16gateserve" --socket "$SCRATCH/vb.sock" --vb-backend file \
  --vb-volume p16gatevol --vb-size-bytes 536870912 \
  --vb-base-dir "$SCRATCH/wal" --vb-file-dir "$SCRATCH/objects" \
  > "$SCRATCH/serve.log" 2>&1 &
sleep 3
pgrep -x p16gateserve > /dev/null || {
  echo "=== P16 LATENCY GATE FAIL === (serve did not start)"; exit 1; }

echo "P16 gate: booting rig guest"
/usr/libexec/qemu-kvm -machine q35,accel=kvm,memory-backend=mem0 \
  -object memory-backend-memfd,id=mem0,size=2048M,share=on -m 2048 -smp 4 \
  -display none -daemonize -pidfile "$QEMU_PID_FILE" \
  -serial "file:$SCRATCH/serial.log" \
  -device virtio-blk-pci,drive=osdisk,bootindex=0 \
  -drive "id=osdisk,file=$RIG_IMAGE,if=none" \
  -chardev "socket,id=vub0,path=$SCRATCH/vb.sock" \
  -device vhost-user-blk-pci,chardev=vub0,num-queues=1,bootindex=1 \
  -netdev "user,id=n0,hostfwd=tcp:127.0.0.1:$SSH_PORT-:22" \
  -device virtio-net-pci,netdev=n0 2> "$SCRATCH/qemu-err.log" || {
  echo "=== P16 LATENCY GATE FAIL === (qemu launch failed)"; exit 1; }

sshq() {
  ssh -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
    -o ConnectTimeout=5 -o BatchMode=yes -i "$RIG_SSH_KEY" -p "$SSH_PORT" \
    rig@127.0.0.1 "$@"
}
guest_up=0
for _ in $(seq 1 50); do
  sleep 8
  if sshq 'true' 2>/dev/null; then guest_up=1; break; fi
done
[ "$guest_up" = 1 ] || { echo "=== P16 LATENCY GATE FAIL === (guest never came up)"; exit 1; }

echo "P16 gate: prefill"
sshq 'sudo fio --name=prefill --filename=/dev/vdb --rw=write --bs=1M --direct=1 --size=512M --numjobs=1 --group_reporting > /dev/null 2>&1; sync'

worst_us=0
fail=0
for leg in $(seq 1 "$LEGS"); do
  max_us=$(sshq 'sudo fio --name=g --filename=/dev/vdb --direct=1 --rw=randwrite --bs=4k --iodepth=1 --numjobs=1 --runtime=30 --time_based --group_reporting 2>/dev/null' \
    | grep -oE "clat \(usec\): min=[0-9]+, max=[0-9]+" | grep -oE "max=[0-9]+" | cut -d= -f2)
  if [ -z "$max_us" ]; then echo "leg $leg: NO RESULT"; fail=1; continue; fi
  echo "leg $leg: clat max = $max_us us (bound $BOUND_US)"
  [ "$max_us" -gt "$worst_us" ] && worst_us=$max_us
  [ "$max_us" -gt "$BOUND_US" ] && fail=1
  sleep 2
done

echo "P16 gate: worst leg clat max = $worst_us us across $LEGS legs"
if [ "$fail" = 0 ]; then
  echo "=== P16 LATENCY GATE PASS ==="
else
  echo "=== P16 LATENCY GATE FAIL ==="
  exit 1
fi
