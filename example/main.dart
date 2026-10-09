/// dbkit_sync tour: offline writes, pull-then-push, deletes, custom backends.
///
/// Run with: `dart run example/main.dart`
library;

import 'package:dbkit/dbkit.dart';
import 'package:dbkit_sync/dbkit_sync.dart';

Future<void> _schema(Db db) async {
  await db.createTable('notes', (t) {
    t.text('id').primary();
    t.text('body').nullable();
    t.text('updated_at').nullable();
  });
}

Future<void> main() async {
  // 1. Local db + schema. Use TEXT ids for sync (no autoincrement clashes).
  final db = Db.memory();
  await _schema(db);

  // 2. Pick a backend. MemorySyncBackend is a fake server; swap in
  //    Firestore / Supabase / REST / custom without changing engine code.
  final server = MemorySyncBackend();
  final sync = await DbSync.init(
    db: db,
    backend: server,
    tables: ['notes'],
  );
  sync.status.listen((e) => print('status: $e'));

  // 3. Write through sync.table() — every write is queued in _sync_outbox.
  final notes = sync.table('notes');
  await notes.insert(
      {'id': newSyncId(), 'body': 'offline first', 'updated_at': nowIso()});
  print('pending: ${await sync.pendingCount()}');

  // 4. Pull, then push. A second device sees the row after it syncs.
  await sync.sync();
  print('pending after sync: ${await sync.pendingCount()}');

  final dbB = Db.memory();
  await _schema(dbB);
  final syncB = await DbSync.init(db: dbB, backend: server, tables: ['notes']);
  await syncB.sync();
  print('device B rows: ${await dbB.table('notes').selectAll()}');

  // 5. Deletes propagate as tombstones.
  final id = (await db.table('notes').selectAll()).first['id'] as String;
  await notes.deleteById(id);
  await sync.sync();
  await syncB.sync();
  print('device B after delete: ${await dbB.table('notes').selectAll()}');

  // 6. Custom backend (any server): implement two closures.
  final custom = CustomSyncBackend(
    onPush: (changes) async => print('push ${changes.length} change(s)'),
    onPull: (req) async => <SyncChange>[],
  );
  final syncC = await DbSync.init(db: db, backend: custom, tables: ['notes']);
  await syncC.sync();

  // 7. Firestore / Supabase wiring lives in your app (no SDK dep here) —
  //    see README §7 and example/custom_backend.dart:
  //
  // FirestoreSyncBackend(
  //   write: (t, id, data) =>
  //       FirebaseFirestore.instance.collection(t).doc(id).set(data),
  //   delete: (t, id) =>
  //       FirebaseFirestore.instance.collection(t).doc(id).delete(),
  //   listSince: (t, since) async { ... },
  // )
  //
  // SupabaseSyncBackend(
  //   upsert: (t, rows) => supabase.from(t).upsert(rows),
  //   deleteByIds: (t, ids) => supabase.from(t).delete().inFilter('id', ids),
  //   fetchSince: (t, since, limit) async { ... },
  // )

  await sync.dispose();
  await syncB.dispose();
  await syncC.dispose();
  await db.close();
  await dbB.close();
}
