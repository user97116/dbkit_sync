/// Two databases syncing through one shared backend (two devices, one server).
///
/// Run with: `dart run example/two_devices.dart`
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
  // One shared "server", two local databases.
  final server = MemorySyncBackend();

  final dbA = Db.memory();
  await _schema(dbA);
  final a = await DbSync.init(db: dbA, backend: server, tables: ['notes']);

  final dbB = Db.memory();
  await _schema(dbB);
  final b = await DbSync.init(db: dbB, backend: server, tables: ['notes']);

  // Device A works offline, then syncs.
  await a.table('notes').insert(
      {'id': newSyncId(), 'body': 'from A (1)', 'updated_at': nowIso()});
  await a.table('notes').insert(
      {'id': newSyncId(), 'body': 'from A (2)', 'updated_at': nowIso()});
  print('A pending before sync: ${await a.pendingCount()}'); // 2
  final ra = await a.sync();
  print('A sync: $ra'); // SyncResult(pushed=2, pulled=0)

  // Device B pulls both rows.
  final rb = await b.sync();
  print('B sync: $rb'); // SyncResult(pushed=0, pulled=2)
  print('B rows: ${await dbB.table('notes').pluck<String>('body')}');

  // Device B edits one row; device A picks it up on its next sync.
  final target =
      (await dbB.table('notes').selectAll()).first['id'] as String;
  await b.table('notes').updateById(target, {'body': 'edited on B'});
  await b.sync();
  await a.sync();
  print('A rows: ${await dbA.table('notes').pluck<String>('body')}');

  await a.dispose();
  await b.dispose();
  await dbA.close();
  await dbB.close();
}
