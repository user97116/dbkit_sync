/// Custom backends: a tiny map-backed server, plus Firestore/Supabase wiring.
///
/// Run with: `dart run example/custom_backend.dart`
library;

import 'package:dbkit/dbkit.dart';
import 'package:dbkit_sync/dbkit_sync.dart';

/// Minimal in-memory "server" with tombstones, built from two closures.
/// Pull returns changes newer than [SyncPullRequest.since], ascending.
class MapServer {
  final store = <String, Map<String, Object?>>{};
  final times = <String, DateTime>{};
  final tombstones = <String, DateTime>{};

  String _key(String table, String rowId) => '$table#$rowId';

  Future<void> push(List<SyncChange> changes) async {
    for (final c in changes) {
      final k = _key(c.table, c.rowId);
      if (c.op == SyncOperation.delete) {
        store.remove(k);
        times.remove(k);
        tombstones[k] = c.updatedAt.toUtc();
      } else {
        store[k] = Map<String, Object?>.from(c.data ?? {});
        times[k] = c.updatedAt.toUtc();
        tombstones.remove(k);
      }
    }
  }

  Future<List<SyncChange>> pull(SyncPullRequest req) async {
    final out = <SyncChange>[];
    store.forEach((k, data) {
      if (!k.startsWith('${req.table}#')) return;
      final t = times[k]!;
      if (req.since != null && !t.isAfter(req.since!)) return;
      out.add(SyncChange(
          table: req.table,
          rowId: k.split('#').last,
          op: SyncOperation.upsert,
          data: Map<String, Object?>.from(data),
          updatedAt: t));
    });
    tombstones.forEach((k, t) {
      if (!k.startsWith('${req.table}#')) return;
      if (req.since != null && !t.isAfter(req.since!)) return;
      out.add(SyncChange(
          table: req.table,
          rowId: k.split('#').last,
          op: SyncOperation.delete,
          updatedAt: t));
    });
    out.sort((x, y) => x.updatedAt.compareTo(y.updatedAt));
    return out;
  }
}

Future<void> main() async {
  final server = MapServer();
  final backend =
      CustomSyncBackend(onPush: server.push, onPull: server.pull);

  final dbA = Db.memory();
  await dbA.createTable('notes', (t) {
    t.text('id').primary();
    t.text('body').nullable();
  });
  final a = await DbSync.init(db: dbA, backend: backend, tables: ['notes']);

  final dbB = Db.memory();
  await dbB.createTable('notes', (t) {
    t.text('id').primary();
    t.text('body').nullable();
  });
  final b = await DbSync.init(db: dbB, backend: backend, tables: ['notes']);

  await a.table('notes').insert({'id': newSyncId(), 'body': 'via custom'});
  print('A sync: ${await a.sync()}');
  print('B sync: ${await b.sync()}');
  print('B rows: ${await dbB.table('notes').selectAll()}');

  // Firestore wiring (in your app, with cloud_firestore imported):
  //
  // final firestore = FirestoreSyncBackend(
  //   write: (table, id, data) => FirebaseFirestore.instance
  //       .collection(table).doc(id).set(data),
  //   delete: (table, id) => FirebaseFirestore.instance
  //       .collection(table).doc(id).delete(),
  //   listSince: (table, since) async {
  //     var q = FirebaseFirestore.instance
  //         .collection(table).orderBy('updated_at').limit(500);
  //     if (since != null) {
  //       q = q.where('updated_at', isGreaterThan: since.toIso8601String());
  //     }
  //     final snap = await q.get();
  //     return snap.docs.map((d) => {'id': d.id, ...d.data()}).toList();
  //   },
  // );
  //
  // Supabase wiring (in your app, with supabase_flutter imported):
  //
  // final supabaseBackend = SupabaseSyncBackend(
  //   upsert: (table, rows) => supabase.from(table).upsert(rows),
  //   deleteByIds: (table, ids) =>
  //       supabase.from(table).delete().inFilter('id', ids),
  //   fetchSince: (table, since, limit) async {
  //     var q = supabase.from(table).select().order('updated_at').limit(limit);
  //     if (since != null) q = q.gt('updated_at', since.toIso8601String());
  //     final res = await q;
  //     return (res as List).map((e) => Map<String, Object?>.from(e)).toList();
  //   },
  // );
  //
  // REST wiring (any HTTP sync server speaking POST /push + GET /pull):
  //
  // final rest = RestSyncBackend(
  //   baseUrl: 'https://api.example.com/sync',
  //   headers: {'Authorization': 'Bearer ...'},
  // );

  await a.dispose();
  await b.dispose();
  await dbA.close();
  await dbB.close();
}
