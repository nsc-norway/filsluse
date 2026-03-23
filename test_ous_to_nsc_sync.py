#!/usr/bin/env python3
import os
import tempfile
import shutil
import time
import unittest
from unittest.mock import patch, MagicMock

# Import the module to test
import ous_to_nsc_sync as sync


class TestOusToNscSync(unittest.TestCase):
    """Test suite for the OUS to NSC sync script."""

    def setUp(self):
        """Create temporary directories for testing."""
        self.test_dir = tempfile.mkdtemp()
        self.ous_root = os.path.join(self.test_dir, "ous-fx")
        self.boston_root = os.path.join(self.test_dir, "boston")
        
        os.makedirs(self.ous_root)
        os.makedirs(self.boston_root)
        
        # Patch the timing and locking for tests
        self.dir_min_age_patcher = patch.object(sync, 'DIR_MIN_AGE', 1)
        self.dir_min_age_patcher.start()
        
        # Avoid real locking with a temp lock file
        self.lockfile = os.path.join(self.test_dir, 'lockfile.lock')
        self.lockfile_patcher = patch.object(sync, 'LOCKFILE', self.lockfile)
        self.lockfile_patcher.start()

    def tearDown(self):
        """Clean up temporary directories."""
        self.dir_min_age_patcher.stop()
        self.lockfile_patcher.stop()
        
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_is_dir_old_enough(self):
        """Test directory age checking."""
        test_dir = os.path.join(self.test_dir, "test_age")
        os.makedirs(test_dir)
        
        # Just created, should not be old enough
        self.assertFalse(sync.is_dir_old_enough(test_dir, 10))
        
        # Set mtime to 15 seconds ago
        old_time = time.time() - 15
        os.utime(test_dir, (old_time, old_time))
        
        # Now it should be old enough
        self.assertTrue(sync.is_dir_old_enough(test_dir, 10))
        self.assertFalse(sync.is_dir_old_enough(test_dir, 20))

    @patch('ous_to_nsc_sync.is_mft_complete_file')
    def test_move_ready_files_ous_to_boston(self, mock_is_complete):
        """Test moving completed files from OUS to Boston."""
        # Setup: Create OUS directory with files
        ous_leaf = os.path.join(self.ous_root, "project1", "Til NSC")
        os.makedirs(ous_leaf)
        
        # Create test files
        file1 = os.path.join(ous_leaf, "completed.txt")
        file2 = os.path.join(ous_leaf, "incomplete.txt")
        
        with open(file1, "w") as f:
            f.write("completed content")
        with open(file2, "w") as f:
            f.write("incomplete content")
        
        # Mock: file1 is complete, file2 is not
        mock_is_complete.side_effect = lambda path, fallback_min_age: path == file1
        
        # Execute
        dst_leaf = os.path.join(self.boston_root, "project1", "Til_NSC")
        moved = sync.move_ready_files_ous_to_boston(ous_leaf, dst_leaf, dry_run=False)
        
        # Verify
        self.assertEqual(moved, 1)
        self.assertTrue(os.path.exists(os.path.join(dst_leaf, "completed.txt")))
        self.assertFalse(os.path.exists(os.path.join(dst_leaf, "incomplete.txt")))
        self.assertFalse(os.path.exists(file1))  # Source should be moved
        self.assertTrue(os.path.exists(file2))   # Incomplete should remain

    @patch('ous_to_nsc_sync.is_mft_complete_file')
    def test_move_ready_files_with_subdirectories(self, mock_is_complete):
        """Test moving files from nested subdirectories."""
        # Setup: Create nested structure
        ous_leaf = os.path.join(self.ous_root, "project1", "Til NSC")
        subdir = os.path.join(ous_leaf, "subdir1", "subdir2")
        os.makedirs(subdir)
        
        file_path = os.path.join(subdir, "data.txt")
        with open(file_path, "w") as f:
            f.write("test data")
        
        # All files are complete
        mock_is_complete.return_value = True
        
        # Execute
        dst_leaf = os.path.join(self.boston_root, "project1", "Til_NSC")
        moved = sync.move_ready_files_ous_to_boston(ous_leaf, dst_leaf, dry_run=False)
        
        # Verify
        self.assertEqual(moved, 1)
        expected_dst = os.path.join(dst_leaf, "subdir1", "subdir2", "data.txt")
        self.assertTrue(os.path.exists(expected_dst))
        self.assertFalse(os.path.exists(file_path))

    @patch('ous_to_nsc_sync.is_mft_complete_file')
    def test_move_ready_files_dry_run(self, mock_is_complete):
        """Test dry run mode doesn't actually move files."""
        # Setup
        ous_leaf = os.path.join(self.ous_root, "project1", "Til NSC")
        os.makedirs(ous_leaf)
        
        file_path = os.path.join(ous_leaf, "test.txt")
        with open(file_path, "w") as f:
            f.write("test content")
        
        mock_is_complete.return_value = True
        
        # Execute with dry_run=True
        dst_leaf = os.path.join(self.boston_root, "project1", "Til_NSC")
        moved = sync.move_ready_files_ous_to_boston(ous_leaf, dst_leaf, dry_run=True)
        
        # Verify: count returned but file not moved
        self.assertEqual(moved, 1)
        self.assertTrue(os.path.exists(file_path))  # Still in source
        self.assertFalse(os.path.exists(dst_leaf))  # Destination not created

    def test_prune_empty_dirs(self):
        """Test pruning of empty directories."""
        # Create directory structure
        base = os.path.join(self.ous_root, "project1")
        empty_dir1 = os.path.join(base, "empty1")
        empty_dir2 = os.path.join(base, "empty2")
        non_empty_dir = os.path.join(base, "non_empty")
        
        os.makedirs(empty_dir1)
        os.makedirs(empty_dir2)
        os.makedirs(non_empty_dir)
        
        # Add a file to non_empty_dir
        with open(os.path.join(non_empty_dir, "file.txt"), "w") as f:
            f.write("content")
        
        # Make directories old enough
        old_time = time.time() - 10
        os.utime(empty_dir1, (old_time, old_time))
        os.utime(empty_dir2, (old_time, old_time))
        
        # Execute pruning
        sync.prune_empty_dirs(self.ous_root, min_age=1, dry_run=False)
        
        # Verify
        self.assertFalse(os.path.exists(empty_dir1))
        self.assertFalse(os.path.exists(empty_dir2))
        self.assertTrue(os.path.exists(non_empty_dir))
        self.assertTrue(os.path.exists(base))  # Parent should remain

    def test_prune_empty_dirs_dry_run(self):
        """Test dry run mode for pruning."""
        empty_dir = os.path.join(self.ous_root, "project1", "empty")
        os.makedirs(empty_dir)
        
        # Make it old
        old_time = time.time() - 10
        os.utime(empty_dir, (old_time, old_time))
        
        # Execute with dry_run=True
        sync.prune_empty_dirs(self.ous_root, min_age=1, dry_run=True)
        
        # Directory should still exist
        self.assertTrue(os.path.exists(empty_dir))

    @patch('ous_to_nsc_sync.os.stat')
    def test_is_mft_complete_file_during_transfer(self, mock_stat):
        """Test MFT completion detection during transfer."""
        # All timestamps equal = still transferring
        mock_obj = MagicMock()
        mock_obj.st_mtime_ns = 1000000000000
        mock_obj.st_ctime_ns = 1000000000000
        mock_stat.return_value = mock_obj
        
        with patch('ous_to_nsc_sync.time.time_ns', return_value=1000000000000):
            self.assertFalse(sync.is_mft_complete_file("/fake/path", fallback_min_age=900))

    @patch('ous_to_nsc_sync.os.stat')
    def test_is_mft_complete_file_after_transfer(self, mock_stat):
        """Test MFT completion detection after transfer."""
        # ctime < mtime = transfer complete
        mock_obj = MagicMock()
        mock_obj.st_mtime_ns = 1000000000001
        mock_obj.st_ctime_ns = 999999999999
        mock_stat.return_value = mock_obj
        
        self.assertTrue(sync.is_mft_complete_file("/fake/path", fallback_min_age=900))

    @patch('ous_to_nsc_sync.os.stat')
    def test_is_mft_complete_file_mtime_greater_than_ctime(self, mock_stat):
        """Test MFT completion when mtime > ctime."""
        # mtime > ctime = complete
        mock_obj = MagicMock()
        mock_obj.st_mtime_ns = 1000000000001
        mock_obj.st_ctime_ns = 1000000000000
        mock_stat.return_value = mock_obj
        
        self.assertTrue(sync.is_mft_complete_file("/fake/path", fallback_min_age=900))
        
        # If mtime == ctime, not complete
        mock_obj.st_mtime_ns = 1000000000000
        mock_obj.st_ctime_ns = 1000000000000
        with patch('ous_to_nsc_sync.time.time_ns', return_value=1000000000000):
            self.assertFalse(sync.is_mft_complete_file("/fake/path", fallback_min_age=900))

    @patch('ous_to_nsc_sync.os.stat')
    def test_is_mft_complete_file_fallback_timeout(self, mock_stat):
        """Equal timestamps become complete once fallback age is exceeded."""
        mock_obj = MagicMock()
        mock_obj.st_mtime_ns = 1000000000000
        mock_obj.st_ctime_ns = 1000000000000
        mock_stat.return_value = mock_obj

        with patch('ous_to_nsc_sync.time.time_ns', return_value=1000000000800):
            self.assertFalse(sync.is_mft_complete_file("/fake/path", fallback_min_age=900))

        with patch('ous_to_nsc_sync.time.time_ns', return_value=1000000000900):
            self.assertTrue(sync.is_mft_complete_file("/fake/path", fallback_min_age=900))

    def test_main_runs_with_test_transfer_jobs(self):
        """Test main executes with patched TRANSFER_JOBS."""
        # Create test OUS leaf with files
        ous_leaf = os.path.join(self.ous_root, "project1", "Til NSC")
        os.makedirs(ous_leaf)
        
        file_path = os.path.join(ous_leaf, "data.txt")
        with open(file_path, "w") as f:
            f.write("test data")
        
        # Create corresponding Boston destination path
        boston_leaf = os.path.join(self.boston_root, "project1", "Til_NSC")
        
        # Create test transfer jobs that point to test directories
        test_transfer_jobs = [
            (ous_leaf, boston_leaf),
        ]
        
        # Patch TRANSFER_JOBS and args for dry-run mode
        with patch.object(sync, 'TRANSFER_JOBS', test_transfer_jobs):
            with patch('sys.argv', ['prog', '--dry-run']):
                # Mock lock acquisition and file completion check, and os.close
                with patch.object(sync, 'acquire_lock', return_value=3):
                    with patch('ous_to_nsc_sync.is_mft_complete_file', return_value=True):
                        with patch('os.close'):
                            sync.main()
        
        # In dry-run mode, nothing should have been moved
        self.assertTrue(os.path.exists(ous_leaf))
        self.assertFalse(os.path.exists(boston_leaf))

    def test_main_moves_files_with_live_transfer_jobs(self):
        """Test main actually moves files in live mode."""
        # Create test OUS leaf with files
        ous_leaf = os.path.join(self.ous_root, "project1", "Til NSC")
        os.makedirs(ous_leaf)
        
        file_path = os.path.join(ous_leaf, "data.txt")
        with open(file_path, "w") as f:
            f.write("test data")
        
        # Create corresponding Boston destination path
        boston_leaf = os.path.join(self.boston_root, "project1", "Til_NSC")
        
        # Create test transfer jobs that point to test directories
        test_transfer_jobs = [
            (ous_leaf, boston_leaf),
        ]
        
        # Patch TRANSFER_JOBS and args for live mode
        with patch.object(sync, 'TRANSFER_JOBS', test_transfer_jobs):
            with patch('sys.argv', ['prog']):
                # Mock lock acquisition and file completion check, and os.close
                with patch.object(sync, 'acquire_lock', return_value=3):
                    with patch('ous_to_nsc_sync.is_mft_complete_file', return_value=True):
                        with patch('os.close'):
                            sync.main()
        
        # Files should have been moved (empty subdir remains until prune is called)
        self.assertTrue(os.path.exists(os.path.join(boston_leaf, 'data.txt')))
        # Original file should be removed from source
        self.assertFalse(os.path.exists(file_path))


if __name__ == "__main__":
    unittest.main()
