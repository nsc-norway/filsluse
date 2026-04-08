#!/usr/bin/env python3
import yaml
import os
import shutil
import sys
import time
import argparse
import fcntl
import logging

# --- Configuration ---


# How long dirs must have been untouched before we consider deleting them (seconds)
DIR_MIN_AGE = 300  # 5 minutes
# If the file timestamp heuristic indicates an ongoing transfer, use this fallback
FILE_FALLBACK_MIN_AGE = 900 # 15 minutes

LOCKFILE = "/var/lock/filsluse/ous_to_nsc_sync.lock"


# --- Config loading ---

def load_config(config_path: str) -> list:
    """
    Load transfer jobs from a YAML config file.
    Returns a list of (src, dst) tuples.
    """
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return [(job["src"], job["dst"]) for job in config["transfer_jobs"]]


# --- Helper functions ---

def get_stat_times(path: str):
    """
    Return (mtime_ns, ctime_ns) as integers (nanoseconds since epoch).
    """
    stat_info = os.stat(path)
    return stat_info.st_mtime_ns, stat_info.st_ctime_ns


def is_mft_complete_file(path: str, fallback_min_age: int) -> bool:
    """
    Detect if the MFT has finished writing 'path' based on stat timestamps.

    Rule:
      - During transfer: mtime == ctime (equal means still writing)
      - After completion: mtime != ctime (not equal means transfer is done)
      - If the file is older than the specified fallback_min_age we consider it complete regardless of timestamps.

    """
    try:
        mtime_ns, ctime_ns = get_stat_times(path)
    except Exception as e:
        logging.warning(f"Could not stat {path}: {e}")
        return False

    if mtime_ns != ctime_ns:
        return True
    else:
        return (time.time_ns() - mtime_ns) >= fallback_min_age


def is_dir_old_enough(path: str, min_age: int) -> bool:
    """Check if directory mtime is at least min_age seconds ago."""
    try:
        mtime = os.path.getmtime(path)
    except FileNotFoundError:
        return False
    return (time.time() - mtime) >= min_age


def move_ready_files_ous_to_boston(ous_leaf: str, dst_leaf: str, dry_run: bool) -> int:
    """
    Move all MFT-complete files from a given OUS Til NSC leaf to the matching
    Boston Til_NSC leaf. Returns number of files moved.
    """
    moved = 0
    for dirpath, dirnames, filenames in os.walk(ous_leaf):
        rel_dir = os.path.relpath(dirpath, ous_leaf)
        if rel_dir == ".":
            dest_dirpath = dst_leaf
        else:
            dest_dirpath = os.path.join(dst_leaf, rel_dir)

        if not dry_run:
            os.makedirs(dest_dirpath, exist_ok=True)

        for fname in filenames:
            src_file = os.path.join(dirpath, fname)
            if not is_mft_complete_file(src_file, FILE_FALLBACK_MIN_AGE):
                continue

            dst_file = os.path.join(dest_dirpath, fname)

            if dry_run:
                logging.info(f"[DRY-RUN] Would move {src_file} -> {dst_file}")
                moved += 1
                continue

            try:
                shutil.move(src_file, dst_file)
                moved += 1
            except Exception as e:
                logging.error(f"Failed to move {src_file} -> {dst_file}: {e}")

    return moved


def prune_empty_dirs(root: str, min_age: int, dry_run: bool) -> bool:
    """
    Recursively remove empty directories under root that:
      - are not root path specified
      - are empty
      - have mtime at least min_age seconds old
    
    Returns true if root is deletable (return value should be ignored by the original caller).
    Allows deleting directories with more recent mtime if the mtime was sufficient before deleting its children.
    """

    # If root doesn't exist, it's considered deletable
    if not os.path.exists(root):
        return True

    is_deletable = is_dir_old_enough(root, min_age)

    with os.scandir(root) as entries:
        for entry in entries:
            entry_path = os.path.join(root, entry.name)
            if entry.is_file():
                if entry.name == ".DS_Store" and not dry_run:
                    # Remove Mac crap files
                    os.unlink(entry_path)
                else:
                    # Not allowed to delete directories with files
                    is_deletable = False

            elif entry.is_dir():
                if not prune_empty_dirs(entry_path, min_age, dry_run):
                    # Child dir can't be removed, so we don't remove this one either
                    is_deletable = False
                else:
                    if dry_run:
                        logging.info(f"[DRY-RUN] Would remove directory {entry_path}")
                    else:
                        os.rmdir(entry_path)

    return is_deletable



# --- Locking to avoid parallel runs ---

def acquire_lock(lockfile: str):
    """
    Acquire an exclusive lock on 'lockfile'. Exit if it's already locked.
    """
    fd = os.open(lockfile, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.info(f"Another instance is running (lock {lockfile}). Exiting.")
        sys.exit(0)
    return fd  # keep this fd open until process exit


# --- CLI and main ---

def parse_args():
    parser = argparse.ArgumentParser(
        description="Move completed MFT files from OUS 'Til NSC' to Boston 'Til_NSC'."
    )
    parser.add_argument(
        "config",
        help="Path to YAML config file specifying transfer jobs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not move or delete anything; just print what would be done.",
    )
    parser.add_argument(
        "--dir-min-age",
        type=int,
        default=DIR_MIN_AGE,
        help=f"Minimum age (seconds) before empty dirs are deleted (default: {DIR_MIN_AGE}).",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output (INFO level messages).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    dry_run = args.dry_run
    dir_min_age = args.dir_min_age

    # Configure logging
    log_level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s'
    )

    mode = "DRY-RUN" if dry_run else "LIVE"
    logging.info(f"OUS→NSC sync starting in {mode} mode")

    transfer_jobs = load_config(args.config)

    # Ensure only one instance runs at a time
    lock_fd = acquire_lock(LOCKFILE)

    # Main moving loop
    total_moved = 0
    for src_path, dst_path in transfer_jobs:
        if dry_run:
            logging.info(f"OUS→Boston: {src_path} -> {dst_path}")
        moved = move_ready_files_ous_to_boston(src_path, dst_path, dry_run)
        total_moved += moved

        # Clean up old empty dirs on both trees
        prune_empty_dirs(src_path, dir_min_age, dry_run)
        prune_empty_dirs(dst_path, dir_min_age, dry_run)

    logging.info(f"Files moved OUS→Boston: {total_moved}")

    # Keep lock_fd open until here; it will be released when process exits
    os.close(lock_fd)
    logging.info("OUS→NSC sync finished")


if __name__ == "__main__":
    main()
