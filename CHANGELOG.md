# Changelog

## 0.1.0

- Initial release: offline-first outbox + pull engine for dbkit.
- `DbSync` with push-then-pull, per-table cursors, LWW / local-wins /
  remote-wins / custom conflict resolution, status stream, auto-sync timer.
- `SyncTableRef` tracked writes; `SyncTrackingAdapter` transparent adapter.
- Backends: `MemorySyncBackend`, `FirestoreSyncBackend` (callback-wired, no
  SDK dep), `SupabaseSyncBackend` (callback-wired), `RestSyncBackend`
  (generic HTTP), `CustomSyncBackend` (two closures).
