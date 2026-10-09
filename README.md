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
2. [Five-minute tour](#2-five-minute-tour)
3. [How it works](#3-how-it-works)
4. [Tracked writes](#4-tracked-writes)
5. [Syncing](#5-syncing)
6. [Conflicts](#6-conflicts)
7. [Backends](#7-backends)
8. [Custom backend](#8-custom-backend)
9. [Flutter wiring](#9-flutter-wiring)
10. [Testing](#10-testing)
11. [Schema tips](#11-schema-tips)
12. [Troubleshooting](#12-troubleshooting)
13. [API reference (one way to do it)](#13-api-reference-one-way-to-do-it)
14. [Project structure & commands](#14-project-structure--commands)

## 1. Setup

```yaml
dependencies:
  dbkit: ^0.4.1
  dbkit_sync: ^0.1.1
```

```dart
import 'package:dbkit/dbkit.dart';
import 'package:dbkit_sync/dbkit_sync.dart';
```

Requires Dart `^3.0.0`. No Firebase/Supabase/HTTP dependencies — you wire your own clients through callbacks (see §7).

## 2. Five-minute tour

```dart
// Schema: TEXT ids, so offline inserts never collide (see §11).
final db = Db.memory();
await db.createTable('notes', (t) {
  t.text('id').primary();
  t.text('body').nullable();
  t.text('updated_at').nullable();
});

// One engine per local database. Tables outside `tables` are never synced.
final sync = await DbSync.init(
  db: db,
  backend: MemorySyncBackend(),
  tables: ['notes'],
);

// Write through sync.table() — queued in _sync_outbox automatically.
final notes = sync.table('notes');
await notes.insert({'id': newSyncId(), 'body': 'hello', 'updated_at': nowIso()});
print(await sync.pendingCount()); // 1

// Pull remotes, then push the queue.
final result = await sync.sync();
print(result); // SyncResult(pushed=1, pulled=0)

await sync.dispose();
```

Run it: `dart run example/main.dart`. More demos in [`example/`](#14-project-structure--commands): two-device sync, conflicts, custom backends.

## 3. How it works

- **Local is truth while offline.** Writes hit SQLite immediately and append one `_sync_outbox` row per affected row (FIFO, durable, survives restarts).
- **`sync()` = pull then push.** Each table first pulls changes newer than its `_sync_state` cursor, resolving conflicts against queued local edits (§6); then outbox batches upload oldest-first in `batchSize` chunks (default 200). Pull-first ordering means a stale local edit can never blindly overwrite a newer remote row on a dumb store.
- **Deletes are tombstones.** A delete enqueues `{op: delete, rowId}` with no data. The backend must retain and serve it so other devices delete too: the memory backend keeps tombstones forever; for Firestore/Supabase prefer a `_deleted` flag (see §7).
- **Cursors are per table** (`table_name → last_pull_at` ISO-8601). After a successful pull the cursor advances to the newest remote `updatedAt` (or now when empty), so interrupted runs resume incrementally. `reset()` clears outbox + cursors; the next pull is a full pull.
- **Backends must be idempotent.** Re-push (`push` retried after a crash) and re-pull (same cursor twice) are safe by contract; every bundled backend is.

Internal tables (created by `ensureSyncSchema`, called inside `DbSync.init`):

```text
_sync_outbox(id, table_name, row_id, op, data JSON, updated_at, attempts, last_error)
_sync_state(table_name PK, last_pull_at)
```

`attempts`/`last_error` record failed pushes; entries stay queued until a push succeeds.

## 4. Tracked writes

Write through `sync.table()` instead of `db.table()` — reads are identical, writes additionally enqueue one outbox entry per affected row:

```dart
final notes = sync.table('notes'); // throws for untracked tables (typo guard)

await notes.insert({'id': newSyncId(), 'body': 'hi'});
await notes.insertMany([{...}, {...}]);
await notes.upsert({'id': 'a', 'body': 'yo'});
await notes.create({'id': newSyncId(), 'body': 'hi'}); // insert + return row
await notes.updateById('a', {'body': 'edited'});
await notes.updateWhere((w) => w.eq('archived', false), {'pinned': true});
await notes.increment('views', where: (w) => w.eq('id', 'a'));
await notes.decrement('views', by: 2, where: (w) => w.eq('id', 'a'));
await notes.deleteById('a');
await notes.deleteWhere((w) => w.lt('views', 3));
await notes.truncate(); // one tombstone per row

// Reads pass straight through (never queued):
await notes.selectAll();
await notes.findById('a');
await notes.findWhere((w) => w.gte('views', 10));
await notes.query().where((w) => w.contains('body', 'hi')).get();
await notes.count();
await notes.paginate(page: 1, perPage: 20);
```

Multi-row writes (`updateWhere`, `deleteWhere`, `increment`, `truncate`) read the affected rows first so each gets its own outbox entry — fine for typical sync tables, but don't point the engine at huge append-only logs.

Existing code that writes via `db.table()` directly is invisible to sync — switch it to `sync.table()`, record it manually, or use the transparent adapter:

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
// Pull-side / maintenance writes must bypass tracking to avoid loops:
await tracker.runUntracked(() async { /* apply remote writes */ });
```

Prefer `sync.table()` for new code; reserve the adapter for retrofitting codebases with scattered `db.table()` writes.

## 5. Syncing

```dart
final result = await sync.sync(); // pull every table, then push
print(result.pushed); // outbox entries uploaded
print(result.pulled); // remote changes applied

await sync.push(); // upload only (batched, oldest first)
await sync.pullTable('notes'); // download one table only
await sync.pendingCount(); // queued entries
await sync.outbox.pending(); // inspect the queue (OutboxEntry list)

sync.status.listen((e) => print(e)); // syncing/pushing/pulling/success/failure
sync.startAutoSync(const Duration(minutes: 5)); // background timer
sync.stopAutoSync();
await sync.reset(); // clear outbox + cursors (next pull is full)
await sync.dispose(); // stop timer, close stream + backend
```

Directions: `SyncDirection.bidirectional` (default), `pushOnly`, `pullOnly` — set in `DbSync.init`. Concurrent `sync()` calls are serialized (the second awaits the in-flight run). A failed push keeps entries queued with bumped `attempts` and rethrows; a failed pull aborts the run without moving that table's cursor.

## 6. Conflicts

A conflict = a remote change arrives for a row with queued local edits.

| Policy | Behaviour |
|---|---|
| `lastWriteWins` (default) | Newest `updatedAt` wins; ties go remote. Superseded local entries are dropped. |
| `localWins` | Remote change is ignored; local entry stays queued. |
| `remoteWins` | Remote applies; queued local entries for that row are dropped. |
| `custom` | Your `resolver` merges; returning `null` keeps local. Merged rows apply locally and re-queue for the next push. |

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

Timestamp source: `row['updated_at']` (configurable via `updatedAtField`) when present, else the outbox/remote clock. Enable `touchUpdatedAt: true` only when every synced table has that column — otherwise inserts fail with "no such column". See `example/conflicts.dart` for a runnable LWW + custom-merge demo.

## 7. Backends

All backends implement two methods — `push(changes)` and `pull(table, since)` — and must be idempotent (re-push / re-pull is safe).

| Backend | Server | Delete propagation | Needs from you |
|---|---|---|---|
| `MemorySyncBackend` | in-process (tests/dev) | tombstones (kept forever) | nothing |
| `FirestoreSyncBackend` | Firestore collections | `_deleted` flag (configurable) | `cloud_firestore` closures |
| `SupabaseSyncBackend` | Supabase tables | `_deleted` flag (configurable) | `supabase_flutter` closures |
| `RestSyncBackend` | your HTTP server | whatever your server keeps | base URL + auth headers |
| `CustomSyncBackend` | anything | your `onPull` decides | two closures |

### In-memory (tests / dev)

```dart
// Share one instance between two DbSyncs = two devices through one server.
final server = MemorySyncBackend();
```

### Firebase (Firestore)

No `cloud_firestore` dependency here — pass closures over your instance. One collection per synced table, same name:

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

Each map from `listSince` must carry the doc id under `idField` (default `'id'`) and its modification time under `updatedAtField` (default `'updated_at'`). Firestore has no native delete feed: use a `_deleted` boolean flag (configurable via `deletedField`, set to `''` to disable) instead of hard deletes when deletes must propagate.

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

Recommended table shape: `id text primary key`, `updated_at timestamptz default now()`, `_deleted bool default false`. Push batches one upsert call and one delete call per table.

### REST (any custom HTTP server)

```dart
RestSyncBackend(baseUrl: 'https://api.example.com/sync', headers: {
  'Authorization': 'Bearer ...',
})
```

Protocol (`SyncChange.toJson` / `SyncChange.fromJson` payloads):

```text
POST /push {"changes": [{table, rowId, op, data, updatedAt}, ...]} -> 2xx
GET  /pull?table=notes&since=<iso>&limit=500 -> {"changes": [...]} (ascending updatedAt)
```

Keep per-user data isolated by scoping paths/headers per authenticated user on your server; the backend just forwards `headers` on every request.

## 8. Custom backend

Anything works — implement two methods directly or wrap closures:

```dart
final backend = CustomSyncBackend(
  onPush: (changes) async { /* upload oldest-first */ },
  onPull: (req) async { /* rows newer than req.since, ascending */ return []; },
);
```

Or `class MyBackend implements SyncBackend { ... }` (remember `dispose()`). See `example/custom_backend.dart` for a runnable in-memory custom server plus copy-paste Firestore/Supabase wiring.

## 9. Flutter wiring

```dart
// Create once (e.g. in app init), dispose on shutdown.
final sync = await DbSync.init(
  db: db,
  backend: SupabaseSyncBackend(/* ... */),
  tables: ['notes'],
  policy: ConflictPolicy.lastWriteWins,
);

// Sync on app resume + on a timer; manual refresh in UI calls sync.sync().
sync.startAutoSync(const Duration(minutes: 5));
sync.status.listen((e) {
  if (e.status == SyncStatus.failure) log('sync failed: ${e.message}');
});

// Optional: trigger a sync when connectivity returns (with connectivity_plus
// in your app — not a dependency of this package).
// connectivity.onConnectivityChanged.listen((_) async {
//   if (!sync.isSyncing) { try { await sync.sync(); } catch (_) {} }
// });
```

Notes:

- One `DbSync` per local database; share it (Provider/Riverpod/get_it) rather than creating engines per screen.
- Failures stay queued — a failed auto-sync is retried on the next tick, and the user can always pull-to-refresh via `sync()`.
- Never create two engines over the same `Db` with different backends: both would drain the same outbox.

## 10. Testing

Use `Db.fake()` (no native libs) + `MemorySyncBackend` (fake server):

```dart
final server = MemorySyncBackend();
final dbA = Db.fake();
final dbB = Db.fake();
// ... createTable on both ...
final a = await DbSync.init(db: dbA, backend: server, tables: ['notes']);
final b = await DbSync.init(db: dbB, backend: server, tables: ['notes']);

await a.table('notes').insert({'id': 'n1', 'body': 'hi'});
await a.sync();
await b.sync();
expect((await dbB.table('notes').findById('n1'))!['body'], 'hi');
```

Two engines sharing one backend = two devices. `server.docCount('notes')` and `sync.pendingCount()` make good assertions. Full suite in `test/sync_test.dart` (`dart test`).

## 11. Schema tips

- **Use string ids** (`t.text('id').primary()` + `newSyncId()`). Integer `AUTOINCREMENT` sequences collide across offline devices; string ids do not.
- **Add `updated_at`** (`t.text('updated_at').nullable()` or `t.timestamps()`) for meaningful last-write-wins. Without it the engine falls back to queue/arrival clocks.
- **Prefer soft deletes** (`_deleted` flag) on Firestore/Supabase so pulls can see deletes; hard deletes need a tombstone table otherwise.
- **One collection/table per synced table**, same name on both sides.
- Keep synced tables narrow: every tracked write stores the full row JSON in the outbox.

## 12. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `sync.table('x')` throws | `x` is not in `DbSync.init(tables: [...])` — typo guard working as intended. |
| Rows never upload (`pendingCount()` grows) | Writes went through `db.table()` instead of `sync.table()` — switch call sites or `recordExternalUpsert` / use `SyncTrackingAdapter`. |
| `DbException: no such column: updated_at` | `touchUpdatedAt: true` but the table lacks the column — add it or leave `touchUpdatedAt: false` (default). |
| Deletes don't propagate | Backend drops tombstones (Firestore hard delete / Supabase hard delete). Use the `_deleted` flag pattern (§7). |
| `MemoryAdapter.execute() ... Use structured APIs` | Raw SQL against `Db.fake()` — the engine never does this; check app code using `raw`/`exec` in tests and run sqlite (`Db.memory()`) instead. |
| Two devices flapping on the same row | Clocks disagree badly and LWW ties flip. Use server-stamped `updated_at` (Firestore `FieldValue.serverTimestamp()` / Supabase `now()`) so one clock decides. |
| Outbox grows forever | `push()` keeps failing (see `outbox.pending()` → `lastError`) or nothing ever calls `sync()` — check status stream + auto-sync. |

## 13. API reference (one way to do it)

One canonical method per job.

| Job | Use this |
|---|---|
| Init + schema | `DbSync.init(db:, backend:, tables:, ...)` |
| Tracked writes | `sync.table('notes').insert/insertMany/upsert/create/updateById/updateWhere/increment/decrement/deleteById/deleteWhere/truncate` |
| Full sync | `sync()` (pull then push) |
| Half sync | `push()` / `pullTable(name)` |
| Backlog | `pendingCount()` / `outbox.pending()` |
| Manual record | `recordExternalUpsert/Delete(...)` |
| Status | `status` stream (`SyncEvent`) / `isSyncing` |
| Background | `startAutoSync(interval)` / `stopAutoSync()` |
| Reset/dispose | `reset()` / `dispose()` |
| Ids/time | `newSyncId()` / `nowIso()` |
| Servers | `MemorySyncBackend` / `FirestoreSyncBackend` / `SupabaseSyncBackend` / `RestSyncBackend` / `CustomSyncBackend` |
| Transparent | `SyncTrackingAdapter(inner, syncedTables: {...})` + `runUntracked` |

## 14. Project structure & commands

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
example/main.dart             # 5-minute tour: write, sync, delete, dispose
example/two_devices.dart      # two databases through one server
example/conflicts.dart        # last-write-wins + custom merge
example/custom_backend.dart   # CustomSyncBackend + Firestore/Supabase wiring
test/sync_test.dart
```

```sh
dart pub get
dart test
dart run example/main.dart
dart run example/two_devices.dart
dart run example/conflicts.dart
dart run example/custom_backend.dart
dart analyze lib test example
```

## License

MIT — see `LICENSE`.
