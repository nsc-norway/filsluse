#!/usr/bin/env python3
import os
import tempfile
import shutil
import time
import unittest
from unittest.mock import patch

# Import the module to test
import nsc_to_ous_sync as sync


class TestNscToOusSync(unittest.TestCase):
    """Test suite for the NSC (Boston) to OUS sync script."""

    def setUp(self):
        """Create temporary directories for testing and patch constants."""
        self.test_dir = tempfile.mkdtemp()
        self.boston_root = os.path.join(self.test_dir, "boston")
        self.ous_root = os.path.join(self.test_dir, "ous-fx")

        os.makedirs(self.boston_root)
        os.makedirs(self.ous_root)

        # Patch the timing to be faster
        self.min_item_age_patcher = patch.object(sync, 'MIN_ITEM_AGE', 1)
        self.dir_min_age_patcher = patch.object(sync, 'DIR_MIN_AGE', 1)

        self.min_item_age_patcher.start()
        self.dir_min_age_patcher.start()

        # Avoid real locking with a temp lock file
        self.lockfile = os.path.join(self.test_dir, 'lockfile.lock')
        self.lockfile_patcher = patch.object(sync, 'LOCKFILE', self.lockfile)
        self.lockfile_patcher.start()

    def tearDown(self):
        """Clean up patches and temporary directories."""
        self.min_item_age_patcher.stop()
        self.dir_min_age_patcher.stop()
        self.lockfile_patcher.stop()

        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def _make_boston_leaf_with_files(self, subpath="project1/runA/Til_Sentrallagring"):
        """Create a Boston leaf directory with test files and subdirs."""
        leaf = os.path.join(self.boston_root, subpath)
        os.makedirs(leaf, exist_ok=True)

        # Files and directories inside the leaf
        file1 = os.path.join(leaf, 'file1.txt')
        file2 = os.path.join(leaf, 'file2.txt')
        subdir = os.path.join(leaf, 'subdir')
        os.makedirs(subdir)
        nested_file = os.path.join(subdir, 'nested.txt')

        with open(file1, 'w') as f:
            f.write('content1')
        with open(file2, 'w') as f:
            f.write('content2')
        with open(nested_file, 'w') as f:
            f.write('nested')

        # Ensure items are old enough
        old = time.time() - 5
        os.utime(file1, (old, old))
        os.utime(file2, (old, old))
        os.utime(subdir, (old, old))
        os.utime(nested_file, (old, old))
        os.utime(leaf, (old, old))

        return leaf, [file1, file2], subdir, nested_file

    def test_move_ready_files_basic(self):
        """Test move_ready_files with simple file transfer."""
        leaf, files, subdir, nested_file = self._make_boston_leaf_with_files()
        
        # Create corresponding OUS destination
        ous_leaf = os.path.join(self.ous_root, "project1", "runA", "Til_Sentrallagring")
        
        # Dry run first: ensure it reports actions
        moved_dry = sync.move_ready_files(
            src_path=leaf,
            dst_path=ous_leaf,
            min_item_age=1,
            dry_run=True,
        )
        self.assertEqual(moved_dry, 3)  # two files + one directory
        # No directories should have been created in dry run
        self.assertFalse(os.path.exists(ous_leaf))
        
        # Live run: perform actual move
        moved_live = sync.move_ready_files(
            src_path=leaf,
            dst_path=ous_leaf,
            min_item_age=1,
            dry_run=False,
        )
        self.assertEqual(moved_live, 3)
        
        # Verify destination items exist
        self.assertTrue(os.path.exists(os.path.join(ous_leaf, 'file1.txt')))
        self.assertTrue(os.path.exists(os.path.join(ous_leaf, 'file2.txt')))
        self.assertTrue(os.path.exists(os.path.join(ous_leaf, 'subdir', 'nested.txt')))
        
        # Verify source files are moved, but empty subdir remains
        # (prune_empty_dirs should be called separately to clean these up)
        self.assertFalse(os.path.exists(os.path.join(leaf, 'file1.txt')))
        self.assertFalse(os.path.exists(os.path.join(leaf, 'file2.txt')))
        self.assertTrue(os.path.exists(subdir))  # Empty subdir remains

    def test_prune_empty_dirs(self):
        """Test that prune_empty_dirs removes empty directories."""
        # Create test directory structure
        empty_dir = os.path.join(self.ous_root, 'projectX', 'emptyY')
        os.makedirs(empty_dir)
        
        # Make directories old enough for pruning
        old = time.time() - 10
        os.utime(empty_dir, (old, old))
        os.utime(os.path.dirname(empty_dir), (old, old))
        
        # Prune
        sync.prune_empty_dirs(self.ous_root, min_age=1, dry_run=False)
        
        # Should be removed
        self.assertFalse(os.path.exists(empty_dir))

    def test_prune_does_not_remove_non_empty_dirs(self):
        """Test that prune_empty_dirs preserves non-empty directories."""
        # Create test directory structure with a file
        test_dir = os.path.join(self.ous_root, 'projectA', 'subA')
        os.makedirs(test_dir)
        test_file = os.path.join(test_dir, 'keep_me.txt')
        
        with open(test_file, 'w') as f:
            f.write('content')
        
        # Make old enough
        old = time.time() - 10
        os.utime(test_file, (old, old))
        os.utime(test_dir, (old, old))
        
        # Prune
        sync.prune_empty_dirs(self.ous_root, min_age=1, dry_run=False)
        
        # Should still exist
        self.assertTrue(os.path.exists(test_dir))
        self.assertTrue(os.path.exists(test_file))

    def test_main_runs_with_test_transfer_jobs(self):
        """Test main executes with patched TRANSFER_JOBS."""
        # Create test leaf with files
        leaf, *_ = self._make_boston_leaf_with_files()
        
        # Create corresponding OUS destination path
        ous_leaf = os.path.join(self.ous_root, "project1", "runA", "Til_Sentrallagring")
        
        # Write a temporary YAML config file
        config_file = os.path.join(self.test_dir, "test_config.yaml")
        with open(config_file, "w") as f:
            f.write(f"transfer_jobs:\n  - src: {leaf}\n    dst: {ous_leaf}\n")
        
        # Patch args for dry-run mode
        with patch('sys.argv', ['prog', config_file, '--dry-run']):
            # Mock lock acquisition to avoid system-level path issues
            with patch.object(sync, 'acquire_lock', return_value=3):
                sync.main()
        
        # In dry-run mode, nothing should have been moved
        self.assertTrue(os.path.exists(leaf))
        self.assertFalse(os.path.exists(ous_leaf))

    def test_main_moves_files_with_live_transfer_jobs(self):
        """Test main actually moves files in live mode."""
        # Create test leaf with files
        leaf, *_ = self._make_boston_leaf_with_files()
        
        # Create corresponding OUS destination path
        ous_leaf = os.path.join(self.ous_root, "project1", "runA", "Til_Sentrallagring")
        
        # Write a temporary YAML config file
        config_file = os.path.join(self.test_dir, "test_config.yaml")
        with open(config_file, "w") as f:
            f.write(f"transfer_jobs:\n  - src: {leaf}\n    dst: {ous_leaf}\n")
        
        # Patch args for live mode
        with patch('sys.argv', ['prog', config_file]):
            # Mock lock acquisition
            with patch.object(sync, 'acquire_lock', return_value=3):
                sync.main()
        
        # Files should have been moved (empty subdir remains until prune is called)
        self.assertTrue(os.path.exists(os.path.join(ous_leaf, 'file1.txt')))
        self.assertTrue(os.path.exists(os.path.join(ous_leaf, 'file2.txt')))
        # Original files should be removed from source
        self.assertFalse(os.path.exists(os.path.join(leaf, 'file1.txt')))
        self.assertFalse(os.path.exists(os.path.join(leaf, 'file2.txt')))



if __name__ == "__main__":
    unittest.main()
