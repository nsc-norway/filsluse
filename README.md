# README – OUS ↔ NSC File Synchronization

## Overview

This service provides automated, bidirectional file transfer between:

- **OUS environment**: `/mnt/ous-fx`
- **Boston / NSC environment**: `/boston/runScratch/OUS-filsluse`

Two cron-driven Python scripts handle transfers in both directions:

- **`ous_to_nsc_sync.py`**: Moves files from OUS `Til NSC` to Boston `Til_NSC`
- **`nsc_to_ous_sync.py`**: Moves files from Boston `Til_Sentrallagring` to OUS `Til Sentrallagring`

Both scripts:
- Detect when files are fully transferred and ready to move
- Clean up empty directory structures after transfers
- Use lock files to prevent concurrent runs
- Support dry-run mode for safe testing

---

## Directory Structure

### OUS to NSC Direction

**Source**: `/mnt/ous-fx/.../Til NSC` (space-separated, case-insensitive)  
**Destination**: `/boston/runScratch/OUS-filsluse/.../Til_NSC` (underscore-separated)

### NSC to OUS Direction

**Source**: `/boston/runScratch/OUS-filsluse/.../Til_Sentrallagring` (underscore-separated)  
**Destination**: `/mnt/ous-fx/.../Til Sentrallagring` (space-separated)


