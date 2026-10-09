# Changelog

## 0.1.1

- Docs: rewrote README (tour, internals, backend comparison, Flutter wiring,
  testing, troubleshooting, full API reference).
- Examples: `main.dart` tour plus new `two_devices.dart`, `conflicts.dart`,
  and `custom_backend.dart` runnable demos.

## 0.1.0

- Initial release: offline-first outbox + pull engine for dbkit.
- `DbSync` with pull-then-push, per-table cursors, LWW / local-wins /
  remote-wins / custom conflict resolution, status stream, auto-sync timer.
- `SyncTableRef` tracked writes; `SyncTrackingAdapter` transparent adapter.
- Backends: `MemorySyncBackend`, `FirestoreSyncBackend` (callback-wired, no
  SDK dep), `SupabaseSyncBackend` (callback-wired), `RestSyncBackend`
  (generic HTTP), `CustomSyncBackend` (two closures).
