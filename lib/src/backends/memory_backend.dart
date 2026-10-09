/// In-memory backend: fake server for tests, examples, and offline dev.
library;

import '../sync_backend.dart';
import '../sync_change.dart';

/// Ephemeral "server" kept in process memory.
///
/// Stores the latest document per `table#id` plus delete tombstones so
/// pulls propagate deletes. Two [DbSync] instances sharing one
/// [MemorySyncBackend] simulate two devices syncing through a server.
///
/// ```dart
/// final server = MemorySyncBackend();
/// final a = await DbSync.init(db: dbA, backend: server, tables: ['notes']);
/// final b = await DbSync.init(db: dbB, backend: server, tables: ['notes']);
/// ```
class MemorySyncBackend implements SyncBackend {
  final Map<String, Map<String, Map<String, Object?>>> _docs = {};
  final Map<String, Map<String, DateTime>> _times = {};
  final Map<String, Map<String, DateTime>> _tombstones = {};

  Map<String, Map<String, Object?>> _tableDocs(String table) =>
      _docs.putIfAbsent(table, () => {});
  Map<String, DateTime> _tableTimes(String table) =>
      _times.putIfAbsent(table, () => {});
  Map<String, DateTime> _tableTombs(String table) =>
      _tombstones.putIfAbsent(table, () => {});

  @override
  Future<void> push(List<SyncChange> changes) async {
    for (final c in changes) {
      final docs = _tableDocs(c.table);
      final times = _tableTimes(c.table);
      final tombs = _tableTombs(c.table);
      if (c.op == SyncOperation.delete) {
        docs.remove(c.rowId);
        times.remove(c.rowId);
        tombs[c.rowId] = c.updatedAt.toUtc();
      } else {
        docs[c.rowId] = Map<String, Object?>.from(c.data ?? {});
        times[c.rowId] = c.updatedAt.toUtc();
        tombs.remove(c.rowId);
      }
    }
  }

  @override
  Future<List<SyncChange>> pull(SyncPullRequest request) async {
    final docs = _tableDocs(request.table);
    final times = _tableTimes(request.table);
    final tombs = _tableTombs(request.table);
    final since = request.since?.toUtc();
    final out = <SyncChange>[];
    docs.forEach((id, data) {
      final t = times[id];
      if (t == null) return;
      if (since != null && !t.isAfter(since)) return;
      out.add(SyncChange(
          table: request.table,
          rowId: id,
          op: SyncOperation.upsert,
          data: Map<String, Object?>.from(data),
          updatedAt: t));
    });
    tombs.forEach((id, t) {
      if (since != null && !t.isAfter(since)) return;
      out.add(SyncChange(
          table: request.table,
          rowId: id,
          op: SyncOperation.delete,
          updatedAt: t));
    });
    out.sort((a, b) => a.updatedAt.compareTo(b.updatedAt));
    if (out.length > request.limit) return out.sublist(0, request.limit);
    return out;
  }

  /// Number of live documents for [table] (test assertions).
  int docCount([String? table]) {
    if (table == null) {
      return _docs.values.fold(0, (n, m) => n + m.length);
    }
    return _docs[table]?.length ?? 0;
  }

  /// Clears all stored documents and tombstones.
  void clear() {
    _docs.clear();
    _times.clear();
    _tombstones.clear();
  }

  @override
  Future<void> dispose() async {}
}
