import 'package:dbkit/dbkit.dart';
import 'package:dbkit_sync/dbkit_sync.dart';
import 'package:test/test.dart';

Future<Db> _notesDb() async {
  final db = Db.fake();
  await db.createTable('notes', (t) {
    t.text('id').primary();
    t.text('body').nullable();
    t.text('updated_at').nullable();
  });
  return db;
}

void main() {
  group('outbox tracking', () {
    test('insert/update/delete enqueue entries', () async {
      final db = await _notesDb();
      final sync = await DbSync.init(
        db: db,
        backend: MemorySyncBackend(),
        tables: ['notes'],
      );
      final notes = sync.table('notes');

      await notes.insert({'id': 'a', 'body': 'hi'});
      expect(await sync.pendingCount(), 1);

      await notes.updateById('a', {'body': 'yo'});
      expect(await sync.pendingCount(), 2);

      await notes.deleteById('a');
      expect(await sync.pendingCount(), 3);

      final pending = await sync.outbox.pending();
      expect(pending.map((e) => e.op).toList(), [
        SyncOperation.upsert,
        SyncOperation.upsert,
        SyncOperation.delete,
      ]);
    });

    test('untracked table throws', () async {
      final db = await _notesDb();
      final sync = await DbSync.init(
        db: db,
        backend: MemorySyncBackend(),
        tables: ['notes'],
      );
      expect(() => sync.table('nope'), throwsArgumentError);
    });

    test('direct db writes are invisible until recorded manually', () async {
      final db = await _notesDb();
      final sync = await DbSync.init(
        db: db,
        backend: MemorySyncBackend(),
        tables: ['notes'],
      );
      await db.table('notes').insert({'id': 'x', 'body': 'raw'});
      expect(await sync.pendingCount(), 0);
      await sync.recordExternalUpsert('notes', {'id': 'x', 'body': 'raw'});
      expect(await sync.pendingCount(), 1);
    });
  });

  group('two-device sync via MemorySyncBackend', () {
    test('push then pull replicates rows', () async {
      final server = MemorySyncBackend();
      final dbA = await _notesDb();
      final dbB = await _notesDb();
      final a = await DbSync.init(db: dbA, backend: server, tables: ['notes']);
      final b = await DbSync.init(db: dbB, backend: server, tables: ['notes']);

      await a.table('notes').insert({'id': 'n1', 'body': 'from A'});
      await a.sync();
      expect(server.docCount('notes'), 1);
      expect(await a.pendingCount(), 0);

      await b.sync();
      expect(await dbB.table('notes').findById('n1'), isNotNull);
      expect((await dbB.table('notes').findById('n1'))!['body'], 'from A');
    });

    test('deletes propagate as tombstones', () async {
      final server = MemorySyncBackend();
      final dbA = await _notesDb();
      final dbB = await _notesDb();
      final a = await DbSync.init(db: dbA, backend: server, tables: ['notes']);
      final b = await DbSync.init(db: dbB, backend: server, tables: ['notes']);

      await a.table('notes').insert({'id': 'd1', 'body': 'bye'});
      await a.sync();
      await b.sync();
      expect(await dbB.table('notes').findById('d1'), isNotNull);

      await a.table('notes').deleteById('d1');
      await a.sync();
      await b.sync();
      expect(await dbB.table('notes').findById('d1'), isNull);
    });

    test('last-write-wins: newer remote overwrites local', () async {
      final server = MemorySyncBackend();
      final dbA = await _notesDb();
      final dbB = await _notesDb();
      final a = await DbSync.init(db: dbA, backend: server, tables: ['notes']);
      final b = await DbSync.init(db: dbB, backend: server, tables: ['notes']);

      await a.table('notes').insert({
        'id': 'c1',
        'body': 'v1',
        'updated_at': '2026-01-01T00:00:00.000Z',
      });
      await a.sync();
      await b.sync();

      // Local edit (older) stays queued on B...
      await dbB.table('notes').updateById('c1', {'body': 'local-old'});
      await b.recordExternalUpsert('notes', {
        'id': 'c1',
        'body': 'local-old',
        'updated_at': '2026-01-02T00:00:00.000Z',
      });
      // ...then a newer remote edit lands on A and pushes.
      await a.table('notes').updateById('c1', {
        'body': 'remote-new',
        'updated_at': '2026-06-01T00:00:00.000Z',
      });
      await a.sync();
      await b.sync();

      final row = await dbB.table('notes').findById('c1');
      expect(row!['body'], 'remote-new');
      // Superseded local entry was dropped.
      expect(await b.pendingCount(), 0);
    });

    test('localWins keeps local row', () async {
      final server = MemorySyncBackend();
      final dbA = await _notesDb();
      final dbB = await _notesDb();
      final a = await DbSync.init(db: dbA, backend: server, tables: ['notes']);
      final b = await DbSync.init(
        db: dbB,
        backend: server,
        tables: ['notes'],
        policy: ConflictPolicy.localWins,
      );

      await a.table('notes').insert({'id': 'w1', 'body': 'base'});
      await a.sync();
      await b.sync();

      await b.table('notes').updateById('w1', {'body': 'mine'});
      await a.table('notes').updateById('w1', {'body': 'theirs'});
      await a.sync();
      await b.sync();

      expect((await dbB.table('notes').findById('w1'))!['body'], 'mine');
    });

    test('custom resolver merges', () async {
      final server = MemorySyncBackend();
      final dbA = await _notesDb();
      final dbB = await _notesDb();
      final a = await DbSync.init(db: dbA, backend: server, tables: ['notes']);
      final b = await DbSync.init(
        db: dbB,
        backend: server,
        tables: ['notes'],
        policy: ConflictPolicy.custom,
        resolver: ({required table, required local, required remote}) async {
          return {
            'id': local['id'],
            'body': '${local['body']}+${remote.data!['body']}',
          };
        },
      );

      await a.table('notes').insert({'id': 'm1', 'body': 'base'});
      await a.sync();
      await b.sync();

      await b.table('notes').updateById('m1', {'body': 'L'});
      await a.table('notes').updateById('m1', {'body': 'R'});
      await a.sync();
      await b.sync();

      expect((await dbB.table('notes').findById('m1'))!['body'], 'L+R');
    });
  });

  group('directions', () {
    test('pushOnly never pulls', () async {
      final server = MemorySyncBackend();
      final dbA = await _notesDb();
      final dbB = await _notesDb();
      final a = await DbSync.init(db: dbA, backend: server, tables: ['notes']);
      final b = await DbSync.init(
        db: dbB,
        backend: server,
        tables: ['notes'],
        direction: SyncDirection.pushOnly,
      );

      await a.table('notes').insert({'id': 'p1', 'body': 'x'});
      await a.sync(); // pushes
      await b.sync(); // pushOnly: pulls nothing
      expect(await dbB.table('notes').findById('p1'), isNull);
    });
  });

  group('pluggable backends', () {
    test('CustomSyncBackend push/pull round-trip', () async {
      final store = <String, Map<String, Object?>>{};
      final backend = CustomSyncBackend(
        onPush: (changes) async {
          for (final c in changes) {
            if (c.op == SyncOperation.delete) {
              store.remove('${c.table}#${c.rowId}');
            } else {
              store['${c.table}#${c.rowId}'] =
                  Map<String, Object?>.from(c.data!);
            }
          }
        },
        onPull: (req) async {
          return store.entries
              .where((e) => e.key.startsWith('${req.table}#'))
              .map((e) => SyncChange(
                    table: req.table,
                    rowId: e.key.split('#').last,
                    op: SyncOperation.upsert,
                    data: e.value,
                    updatedAt: DateTime.now().toUtc(),
                  ))
              .toList();
        },
      );
      final db = await _notesDb();
      final sync =
          await DbSync.init(db: db, backend: backend, tables: ['notes']);
      await sync.table('notes').insert({'id': 'k1', 'body': 'custom'});
      final r = await sync.sync();
      expect(r.pushed, 1);
      expect(store.containsKey('notes#k1'), isTrue);
    });

    test('FirestoreSyncBackend maps ops to callbacks', () async {
      final written = <String, Map<String, Object?>>{};
      final deleted = <String>[];
      final backend = FirestoreSyncBackend(
        write: (t, id, data) async {
          written['$t#$id'] = data;
        },
        delete: (t, id) async {
          deleted.add('$t#$id');
        },
        listSince: (t, since) async => [
          {'id': 'r1', 'body': 'remote', 'updated_at': '2026-03-01T00:00:00.000Z'},
          {
            'id': 'r2',
            'body': 'gone',
            'updated_at': '2026-03-02T00:00:00.000Z',
            '_deleted': true,
          },
        ],
      );
      final db = await _notesDb();
      final sync =
          await DbSync.init(db: db, backend: backend, tables: ['notes']);
      await sync.table('notes').insert({'id': 'l1', 'body': 'local'});
      await sync.table('notes').deleteById('l1');
      await sync.push();
      expect(written.containsKey('notes#l1'), isTrue);
      expect(deleted, contains('notes#l1'));

      final pulled = await sync.pullTable('notes');
      expect(pulled, 1); // r1 applied; r2 tombstone matches nothing
      expect((await db.table('notes').findById('r1'))!['body'], 'remote');
    });

    test('SupabaseSyncBackend batches per table', () async {
      final upserted = <String, List<Map<String, Object?>>>{};
      final removed = <String, List<String>>{};
      final backend = SupabaseSyncBackend(
        upsert: (t, rows) async {
          upserted.putIfAbsent(t, () => []).addAll(rows);
        },
        deleteByIds: (t, ids) async {
          removed.putIfAbsent(t, () => []).addAll(ids);
        },
        fetchSince: (t, since, limit) async => [
          {'id': 's1', 'body': 'hi', 'updated_at': '2026-04-01T00:00:00.000Z'},
        ],
      );
      final db = await _notesDb();
      final sync =
          await DbSync.init(db: db, backend: backend, tables: ['notes']);
      await sync.table('notes').insert({'id': 'a', 'body': 'x'});
      await sync.table('notes').insert({'id': 'b', 'body': 'y'});
      await sync.push();
      expect(upserted['notes']!.length, 2);
      await sync.pullTable('notes');
      expect(await db.table('notes').findById('s1'), isNotNull);
    });
  });

  group('tracking adapter', () {
    test('transparently queues db.table() writes', () async {
      final tracker = SyncTrackingAdapter(
        MemoryAdapter(),
        syncedTables: {'notes'},
      );
      final db = Db.custom(tracker);
      await db.createTable('notes', (t) {
        t.text('id').primary();
        t.text('body').nullable();
      });
      await ensureSyncSchema(db);

      await db.table('notes').insert({'id': 't1', 'body': 'auto'});
      final queued =
          await db.table('_sync_outbox').where((w) => w.eq('row_id', 't1')).get();
      expect(queued.length, 1);

      await tracker.runUntracked(() async {
        await db.table('notes').insert({'id': 't2', 'body': 'quiet'});
      });
      final quiet =
          await db.table('_sync_outbox').where((w) => w.eq('row_id', 't2')).get();
      expect(quiet, isEmpty);
    });
  });

  group('utils', () {
    test('newSyncId is unique', () {
      expect(newSyncId() == newSyncId(), isFalse);
    });
  });
}
