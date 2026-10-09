/// Conflict resolution: last-write-wins (default) and a custom merge function.
///
/// Run with: `dart run example/conflicts.dart`
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

Future<DbSync> _device(Db db, SyncBackend backend,
    {ConflictPolicy policy = ConflictPolicy.lastWriteWins,
    ConflictResolver? resolver}) async {
  return DbSync.init(
    db: db,
    backend: backend,
    tables: ['notes'],
    policy: policy,
    resolver: resolver,
  );
}

Future<void> main() async {
  // -- last-write-wins: the newer updated_at wins, ties go remote ---------
  final server = MemorySyncBackend();

  final dbA = Db.memory();
  await _schema(dbA);
  final a = await _device(dbA, server);

  final dbB = Db.memory();
  await _schema(dbB);
  final b = await _device(dbB, server);

  await a.table('notes').insert({
    'id': 'c1',
    'body': 'base',
    'updated_at': '2026-01-01T00:00:00.000Z',
  });
  await a.sync();
  await b.sync(); // B now has 'base'

  // B edits (older timestamp) but stays offline...
  await b
      .table('notes')
      .updateById('c1', {'body': 'from B', 'updated_at': '2026-02-01T00:00:00.000Z'});
  // ...while A edits newer and pushes.
  await a.table('notes').updateById(
      'c1', {'body': 'from A', 'updated_at': '2026-03-01T00:00:00.000Z'});
  await a.sync();

  // B syncs: pull sees the newer remote first, drops the stale queued edit.
  await b.sync();
  print('LWW winner: ${(await dbB.table('notes').findById('c1'))!['body']}');
  print('B pending after resolve: ${await b.pendingCount()}'); // 0

  // -- custom policy: merge instead of picking a winner --------------------
  final server2 = MemorySyncBackend();

  final dbC = Db.memory();
  await _schema(dbC);
  final c = await _device(dbC, server2);

  final dbD = Db.memory();
  await _schema(dbD);
  final d = await _device(
    dbD,
    server2,
    policy: ConflictPolicy.custom,
    resolver: ({required table, required local, required remote}) async =>
        {'id': local['id'], 'body': '${local['body']}+${remote.data!['body']}'},
  );

  await c.table('notes').insert({'id': 'm1', 'body': 'base'});
  await c.sync();
  await d.sync();

  await d.table('notes').updateById('m1', {'body': 'L'});
  await c.table('notes').updateById('m1', {'body': 'R'});
  await c.sync();
  await d.sync(); // merges to L+R, re-queues, then pushes the merge
  print('merged: ${(await dbD.table('notes').findById('m1'))!['body']}');

  for (final s in [a, b, c, d]) {
    await s.dispose();
  }
  for (final db in [dbA, dbB, dbC, dbD]) {
    await db.close();
  }
}
