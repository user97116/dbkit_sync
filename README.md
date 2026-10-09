# dbkit_sync

Offline-first sync for [**dbkit**](../dbkit): local writes queue in `_sync_outbox`, `DbSync.sync()` pulls remote changes first (resolving conflicts against queued edits), then pushes the queue. Backends are pluggable — **Firebase (Firestore)**, **Supabase**, **REST**, or **any custom server** — without changing engine code, and without this package depending on any SDK.

```dart
import 'package:dbkit/dbkit.dart';
import 'package:dbkit_sync/dbkit_sync.dart';

final db = Db.memory();
await db.createTable('notes', (t) {
  t.text('id').primary();
  t.text('body').nullable();
  t.text('updated_at').nullable();
});

final sync = await DbSync.init(
  db: db,
  backend: MemorySyncBackend(), // swap for Firestore/Supabase/REST/custom
  tables: ['notes'],
);

await sync.table('notes').insert({'id': newSyncId(), 'body': 'offline first'});
await sync.sync();
```

## Contents

1. [Setup](#1-setup)
2. [Core idea](#2-core-idea)
3. [Tracked writes](#3-tracked-writes)
4. [Syncing](#4-syncing)
5. [Conflicts](#5-conflicts)
6. [Backends](#6-backends)
7. [Custom backend](#7-custom-backend)
8. [Schema tips](#8-schema-tips)
9. [API reference](#9-api-reference)
10. [Project structure & commands](#10-project-structure--commands)

## 1. Setup

```yaml
dependencies:
  dbkit:
    path: ../dbkit
  dbkit_sync:
    path: ../dbkit_sync
```

```dart
import 'package:dbkit/dbkit.dart';
import 'package:dbkit_sync/dbkit_sync.dart';
```

Requires Dart `^3.0.0`. No Firebase/Supabase/HTTP dependencies — you wire your own clients through callbacks (see §6).

## 2. Core idea

- **Local is truth while offline.** Writes go to SQLite immediately and are also appended to `_sync_outbox` (FIFO, durable).
- **`sync()` = pull then push.** Each table first pulls changes newer than its `_sync_state` cursor (resolving conflicts against queued local edits), then outbox batches upload oldest-first. Pull-first ordering means a stale local edit can never blindly overwrite a newer remote row on a dumb store.
- **Deletes are tombstones.** A delete enqueues `{op: delete, rowId}`; the backend must retain and serve it so other devices delete too (memory backend keeps tombstones; for Firestore/Supabase prefer a `_deleted` flag — see §6).
- **Cursors are per table** (`table_name → last_pull_at`), so interrupted runs resume incrementally.

## 3. Tracked writes

Write through `sync.table()` instead of `db.table()` — reads are identical, writes additionally enqueue one outbox entry per affected row:

```dart
final notes = sync.table('notes'); // throws for untracked tables (typo guard)

await notes.insert({'id': newSyncId(), 'body': 'hi'});
await notes.upsert({'id': 'a', 'body': 'yo'});
await notes.updateById('a', {'body': 'edited'});
await notes.updateWhere((w) => w.eq('archived', false), {'pinned': true});
await notes.increment('views', where: (w) => w.eq('id', 'a'));
await notes.deleteById('a');
await notes.deleteWhere((w) => w.lt('views', 3));

// Reads pass straight through (never queued):
await notes.selectAll();
await notes.findById('a');
await notes.query().where((w) => w.contains('body', 'hi')).get();
```

Existing code that writes via `db.table()` directly is invisible to sync — either switch it to `sync.table()`, record it manually, or use the transparent adapter:

```dart
await db.table('notes').insert({'id': 'x', 'body': 'raw'});
await sync.recordExternalUpsert('notes', {'id': 'x', 'body': 'raw'});
await sync.recordExternalDelete('notes', 'x');
```

```dart
// Transparent alternative: ALL db.table() writes auto-queue.
final tracker = SyncTrackingAdapter(MemoryAdapter(), syncedTables: {'notes'});
final db = Db.custom(tracker);
await ensureSyncSchema(db);
// Pull-side maintenance must bypass tracking to avoid loops:
await tracker.runUntracked(() async { /* apply remote writes */ });
```

## 4. Syncing

```dart
final result = await sync.sync(); // pull, then push
print(result.pushed); // outbox entries uploaded
print(result.pulled); // remote changes applied

await sync.push(); // upload only
await sync.pullTable('notes'); // download one table only
await sync.pendingCount(); // queued entries

sync.status.listen((e) => print(e)); // idle/syncing/pushing/pulling/success/failure
sync.startAutoSync(const Duration(minutes: 5)); // background timer
sync.stopAutoSync();
await sync.reset(); // clear outbox + cursors (next pull is full)
await sync.dispose(); // stop timer, close stream + backend
```

Directions: `SyncDirection.bidirectional` (default), `pushOnly`, `pullOnly` — set in `DbSync.init`.

## 5. Conflicts

A conflict = remote change arrives for a row with queued local edits.

| Policy | Behaviour |
|---|---|
| `lastWriteWins` (default) | Newest `updatedAt` wins; ties go remote. Superseded local entries are dropped. |
| `localWins` | Remote change is ignored; local entry stays queued. |
| `remoteWins` | Remote applies; queued local entries for that row are dropped. |
| `custom` | Your `resolver` merges; returning `null` keeps local. |

```dart
final sync = await DbSync.init(
  db: db,
  backend: server,
  tables: ['notes'],
  policy: ConflictPolicy.custom,
  resolver: ({required table, required local, required remote}) async {
    // Merge, or return null to keep local.
    return {'id': local['id'], 'body': '${local['body']}+${remote.data!['body']}'};
  },
);
```

Timestamp source: `row['updated_at']` when present, else the outbox/remote clock. Enable `touchUpdatedAt: true` only when every synced table has that column — otherwise inserts fail with "no such column".

## 6. Backends

All backends implement two methods — `push(changes)` and `pull(table, since)` — and must be idempotent (re-push / re-pull is safe).

### In-memory (tests / dev)

```dart
final server = MemorySyncBackend(); // share between two DbSyncs = two devices
```

### Firebase (Firestore)

No `cloud_firestore` dependency here — pass closures over your instance:

```dart
FirestoreSyncBackend(
  write: (table, id, data) =>
      FirebaseFirestore.instance.collection(table).doc(id).set(data),
  delete: (table, id) =>
      FirebaseFirestore.instance.collection(table).doc(id).delete(),
  listSince: (table, since) async {
    var q = FirebaseFirestore.instance
        .collection(table).orderBy('updated_at').limit(500);
    if (since != null) {
      q = q.where('updated_at', isGreaterThan: since.toIso8601String());
    }
    final snap = await q.get();
    return snap.docs.map((d) => {'id': d.id, ...d.data()}).toList();
  },
  // pulled docs with _deleted == true become local deletes
)
```

Firestore has no native delete feed: use a `_deleted` boolean flag (configurable via `deletedField`) instead of hard deletes when you need delete propagation.

### Supabase

Same pattern — pass closures over your `SupabaseClient`:

```dart
SupabaseSyncBackend(
  upsert: (table, rows) => supabase.from(table).upsert(rows),
  deleteByIds: (table, ids) =>
      supabase.from(table).delete().inFilter('id', ids),
  fetchSince: (table, since, limit) async {
    var q = supabase.from(table).select().order('updated_at').limit(limit);
    if (since != null) q = q.gt('updated_at', since.toIso8601String());
    final res = await q;
    return (res as List).map((e) => Map<String, Object?>.from(e)).toList();
  },
)
```

Recommended table shape: `id text primary key`, `updated_at timestamptz default now()`, `_deleted bool default false`.

### REST (any custom HTTP server)

```dart
RestSyncBackend(baseUrl: 'https://api.example.com/sync', headers: {
  'Authorization': 'Bearer ...',
})
```

Protocol: `POST /push {"changes": [...]} → 2xx`; `GET /pull?table=x&since=<iso>&limit=500 → {"changes": [...]}` with `SyncChange.toJson/fromJson` payloads.

## 7. Custom backend

Anything works — implement two methods directly or wrap closures:

```dart
final backend = CustomSyncBackend(
  onPush: (changes) async { /* upload */ },
  onPull: (req) async { /* return changes newer than req.since */ return []; },
);
```

Or `class MyBackend implements SyncBackend { ... }`.

## 8. Schema tips

- **Use string ids** (`t.text('id').primary()` + `newSyncId()`). Integer `AUTOINCREMENT` sequences collide across offline devices; string ids do not.
- **Add `updated_at`** (`t.text('updated_at').nullable()` or `t.timestamps()`) for meaningful last-write-wins. Without it the engine falls back to queue/arrival clocks.
- **Prefer soft deletes** (`_deleted` flag) on Firestore/Supabase so pulls can see deletes; hard deletes need a tombstone table otherwise.
- **One collection/table per synced table**, same name on both sides.

## 9. API reference

| Job | Use this |
|---|---|
| Init + schema | `DbSync.init(db:, backend:, tables:, ...)` |
| Tracked writes | `sync.table('notes').insert/updateById/deleteById/...` |
| Full sync | `sync()` (pull then push) |
| Half sync | `push()` / `pullTable(name)` |
| Backlog | `pendingCount()` / `outbox.pending()` |
| Manual record | `recordExternalUpsert/Delete(...)` |
| Status | `status` stream (`SyncEvent`) |
| Background | `startAutoSync(interval)` / `stopAutoSync()` |
| Reset/dispose | `reset()` / `dispose()` |
| Ids/time | `newSyncId()` / `nowIso()` |
| Servers | `MemorySyncBackend` / `FirestoreSyncBackend` / `SupabaseSyncBackend` / `RestSyncBackend` / `CustomSyncBackend` |
| Transparent | `SyncTrackingAdapter(inner, syncedTables: {...})` + `runUntracked` |

## 10. Project structure & commands

```text
lib/dbkit_sync.dart
lib/src/sync_change.dart      # SyncChange, OutboxEntry, SyncOperation
lib/src/sync_backend.dart     # SyncBackend, SyncPullRequest
lib/src/sync_config.dart      # SyncConfig, SyncDirection, ConflictPolicy
lib/src/sync_engine.dart      # DbSync, SyncResult, SyncStatus
lib/src/synced_table.dart     # SyncTableRef (tracked writes)
lib/src/tracking_adapter.dart # SyncTrackingAdapter (transparent mode)
lib/src/outbox_store.dart     # _sync_outbox / _sync_state
lib/src/utils.dart            # newSyncId, nowIso, ...
lib/src/backends/memory_backend.dart
lib/src/backends/firestore_backend.dart
lib/src/backends/supabase_backend.dart
lib/src/backends/rest_backend.dart  # + CustomSyncBackend
example/main.dart
test/sync_test.dart
```

```sh
dart pub get
dart test
dart run example/main.dart
dart analyze lib test example
```

## License

MIT — see `LICENSE`.
