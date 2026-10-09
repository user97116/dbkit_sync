/// dbkit_sync — offline-first sync for dbkit.
///
/// Local-first: writes queue in `_sync_outbox`, `DbSync.sync` pulls remote
/// changes first (resolving conflicts), then pushes the queue. Backends are pluggable — Firebase
/// (Firestore), Supabase, generic REST, or fully custom.
///
/// ```dart
/// import 'package:dbkit/dbkit.dart';
/// import 'package:dbkit_sync/dbkit_sync.dart';
///
/// final db = Db.memory();
/// final sync = await DbSync.init(
///   db: db,
///   backend: MemorySyncBackend(), // or Firestore/Supabase/REST/custom
///   tables: ['notes'],
/// );
/// await sync.table('notes').insert({'id': newSyncId(), 'body': 'hi'});
/// await sync.sync();
/// ```
library;

export 'src/outbox_store.dart'
    show OutboxStore, SyncStateStore, ensureSyncSchema, kOutboxTable, kStateTable, stringifyId;
export 'src/sync_backend.dart' show SyncBackend, SyncPullRequest;
export 'src/sync_change.dart'
    show SyncChange, OutboxEntry, SyncOperation, syncOperationFromString;
export 'src/sync_config.dart'
    show SyncConfig, SyncDirection, ConflictPolicy, ConflictResolver;
export 'src/sync_engine.dart'
    show DbSync, SyncResult, SyncStatus, SyncEvent;
export 'src/synced_table.dart' show SyncTableRef;
export 'src/tracking_adapter.dart' show SyncTrackingAdapter;
export 'src/utils.dart' show newSyncId, nowIso, extractChangeTime, coerceRowId;
export 'src/backends/memory_backend.dart' show MemorySyncBackend;
export 'src/backends/firestore_backend.dart'
    show
        FirestoreSyncBackend,
        FirestoreRead,
        FirestoreWrite,
        FirestoreDelete,
        FirestoreListSince;
export 'src/backends/supabase_backend.dart'
    show
        SupabaseSyncBackend,
        SupabaseUpsert,
        SupabaseDelete,
        SupabaseFetchSince;
export 'src/backends/rest_backend.dart'
    show RestSyncBackend, CustomSyncBackend;
