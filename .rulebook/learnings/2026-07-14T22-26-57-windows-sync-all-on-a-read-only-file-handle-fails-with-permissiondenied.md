# Windows: sync_all on a read-only file handle fails with PermissionDenied
**Source**: manual
**Date**: 2026-07-14
**Related Task**: phase2_checkpoints
**Tags**: windows, fsync, durability, storage
On Windows, `File::sync_all()` (FlushFileBuffers) requires a handle with write access. Opening a file with `OpenOptions::new().read(true)` and calling `sync_all()` returns Io PermissionDenied (os error 5, "Acesso negado"). To make a freshly copied file durable, open it with `.write(true)` (no truncate) before `sync_all()`. Hit in T2.3's DirectoryArchive (checkpoint/compact.rs); POSIX allows fsync on read-only descriptors, so this only surfaces on the Windows CI/dev machines.