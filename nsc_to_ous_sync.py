#!/usr/bin/env python3
import os
import shutil
import sys
import time
import argparse
import fcntl
import logging

# --- Configuration ---

TRANSFER_JOBS = [
    (
        "/boston/runScratch/OUS-filsluse/UL-AMG-MiSeq/AMG/Til_Sentrallagring",
        "/mnt/ous-fx/UL-AMG-MiSeq/AMG/Til Sentrallagring",
    ),
    (
        "/boston/runScratch/OUS-filsluse/UL-AMG-MiSeq/IMM/Til_Sentrallagring",
        "/mnt/ous-fx/UL-AMG-MiSeq/IMM/Til Sentrallagring",
    ),
    (
        "/boston/runScratch/OUS-filsluse/UL-AMG-MiSeq/MIK/Til_Sentrallagring",
        "/mnt/ous-fx/UL-AMG-MiSeq/MIK/Til Sentrallagring",
    ),
    (
        "/boston/runScratch/OUS-filsluse/UL-AMG-Nanopore/Til_Sentrallagring",
        "/mnt/ous-fx/UL-AMG-Nanopore/Til sentrallagring", # sic
    ),
    (
        "/boston/runScratch/OUS-filsluse/UL-AMG-NextSeq550Dx/Til_Sentrallagring",
        "/mnt/ous-fx/UL-AMG-NextSeq550Dx/Til Sentrallagring"
    ),
    (
        "/boston/runScratch/OUS-filsluse/UL-AMG-NIPTVeriSeq/Til_Sentrallagring",
        "/mnt/ous-fx/UL-AMG-NIPTVeriSeq/Til Sentrallagring",
    ),
    (
        "/boston/runScratch/OUS-filsluse/UL-AMG-NovaSeqX/AMG/Til_Sentrallagring",
        "/mnt/ous-fx/UL-AMG-NovaSeqX/AMG/Til Sentrallagring"
    ),
    (
        "/boston/runScratch/OUS-filsluse/UL-AMG-NovaSeqX/MIK/Til_Sentrallagring",
        "/mnt/ous-fx/UL-AMG-NovaSeqX/MIK/Til Sentrallagring"
    ),
    (
        "/boston/runScratch/OUS-filsluse/UL-AMG-OGM/Til_Sentrallagring",
        "/mnt/ous-fx/UL-AMG-OGM/Til Sentrallagring"
    )
]

EXCLUDE_FILENAMES = set([
    ".DS_Store",
])


BOSTON_ROOT = "/boston/runScratch/OUS-filsluse"
OUS_ROOT = "/mnt/ous-fx"

# How old a file/dir must be (since last modification) before moving (seconds)
MIN_ITEM_AGE = 60

# How old an empty dir must be before deleting (seconds)
DIR_MIN_AGE = 7200  # 2 hours

# Lock file to avoid overlapping runs
LOCKFILE = "/var/lock/filsluse/nsc_to_ous_sync.lock"


# --- Age / readiness helpers ---

def is_path_old_enough(path: str, min_age: int) -> bool:
    """Check if path's mtime is at least min_age seconds in the past."""
    try:
        mtime = os.path.getmtime(path)
    except FileNotFoundError:
        return False
    return (time.time() - mtime) >= min_age


# --- Locking ---

def acquire_lock(lockfile: str):
    """
    Acquire an exclusive lock on lockfile. If already locked, exit.
    """
    fd = os.open(lockfile, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.info(f"Another instance is running (lock {lockfile}). Exiting.")
        sys.exit(0)
    return fd  # keep open until exit


# --- Core move logic ---

def move_ready_files(src_path: str, dst_path: str, min_item_age: int, dry_run: bool) -> int:
    """
    Move all sufficiently old files to the destination.
    Returns the number of files moved.
    """
    moved = 0
    for dirpath, dirnames, filenames in os.walk(src_path):
        rel_dir = os.path.relpath(dirpath, src_path)
        if rel_dir == ".":
            dest_dirpath = dst_path
        else:
            dest_dirpath = os.path.join(dst_path, rel_dir)

        if not dry_run:
            os.makedirs(dest_dirpath, exist_ok=True)

        for fname in filenames:
            src_file = os.path.join(dirpath, fname)

            if fname in EXCLUDE_FILENAMES:
                continue

            if not is_path_old_enough(src_file, min_item_age):
                continue

            dst_file = os.path.join(dest_dirpath, fname)

            if dry_run:
                logging.info(f"[DRY-RUN] Would move {src_file} -> {dst_file}")
                moved += 1
                continue

            try:
                shutil.copyfile(src_file, dst_file)
                os.unlink(src_file)
                moved += 1
            except Exception as e:
                logging.error(f"Failed to move {src_file} -> {dst_file}: {e}")

    return moved


# --- Directory cleanup ---

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

    is_deletable = is_path_old_enough(root, min_age)

    for entry in os.scandir(root):
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
                    logging.info(f"[DRY-RUN] Would remove directory {dirpath}")
                else:
                    os.rmdir(entry_path)

    return is_deletable


# --- CLI & main ---

def parse_args():
    parser = argparse.ArgumentParser(
        description="Move items from NSC/Boston Til_Sentrallagring to OUS Til Sentrallagring using staging."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not move or delete anything; just print what would be done.",
    )
    parser.add_argument(
        "--min-item-age",
        type=int,
        default=MIN_ITEM_AGE,
        help=f"Minimum age (seconds) for files/dirs before moving (default: {MIN_ITEM_AGE}).",
    )
    parser.add_argument(
        "--dir-min-age",
        type=int,
        default=DIR_MIN_AGE,
        help=f"Minimum age (seconds) for empty directories before deleting (default: {DIR_MIN_AGE}).",
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
    min_item_age = args.min_item_age
    dir_min_age = args.dir_min_age

    # Configure logging
    log_level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s'
    )

    mode = "DRY-RUN" if dry_run else "LIVE"
    logging.info(f"NSC→OUS sync starting in {mode} mode "
          f"(min_item_age={min_item_age}s, dir_min_age={dir_min_age}s)")

    # Lock to avoid overlapping runs
    lock_fd = acquire_lock(LOCKFILE)

    # Main transfer loop
    total_moved = 0
    for src_path, dest_path in TRANSFER_JOBS:
        if dry_run:
            logging.info(f"Boston→OUS leaf: {src_path} -> {dest_path}")
        moved_here = move_ready_files(
            src_path, dest_path, min_item_age, dry_run
        )
        total_moved += moved_here

        # Cleanup both source and destination trees
        prune_empty_dirs(src_path, dir_min_age, dry_run)
        prune_empty_dirs(dest_path, dir_min_age, dry_run)

    logging.info(f"Files moved NSC→OUS: {total_moved}")


    #os.close(lock_fd)
    logging.info("NSC→OUS sync finished")


if __name__ == "__main__":
    main()
