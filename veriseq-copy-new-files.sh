#!/usr/bin/env bash
set -euo pipefail

# Script to copy new files from a read-only source to a destination which may delete the
# copied files.

# The source is the Veriseq output folder
# The destination is the intermediate storage for the MFT solution

# * Files in the source should only be added, not removed or modified.

SRC="/mnt/veriseq-server-output/"
STATE="/var/lib/veriseq-transfer/state/"
SENT="/var/lib/veriseq-transfer/sent/"
DROP="/mnt/ous-fx/UL-AMG-NIPTVeriSeq/Til Sentrallagring/veriseq-server-output/prod/"

LOCK="/var/lock/filsluse/veriseq-transfer.lock"

exec 9>"$LOCK"
flock -n 9 || exit 0


# Make an atomic copy of the source directory as the sync baseline for this run.
rsync -a --ignore-existing "$SRC" "$STATE"


# Transfer new files from baseline to the file drop, but only if they haven't already been sent.
rsync -rD \
  --ignore-existing \
  --compare-dest="$SENT" \
  "$STATE" \
  "$DROP"

# After successful transfer, update the sent directory with any new files from the state directory.
rsync -a --ignore-existing "$STATE" "$SENT"
