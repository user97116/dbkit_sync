/// Transparent write-tracking adapter: tracks ALL writes automatically.
library;

import 'dart:convert';

import 'package:dbkit/dbkit.dart';

import 'outbox_store.dart';
import 'sync_change.dart';
import 'utils.dart';

/// A [DbAdapter] decorator that records every write to [syncedTables] into
/// `_sync_outbox` automatically, so callers can keep using plain
/// `db.table()` without switching to `sync.table()`.
///
/// ```dart
/// final inner = Sqlite3Adapter.memory(); // or MemoryAdapter() in tests
/// final tracker = SyncTrackingAdapter(inner, syncedTables: {'notes'});
/// final db = Db.custom(tracker);
/// await DbSync.init(db: db, backend: server, tables: ['notes']);
/// await db.table('notes').insert({'id': newSyncId(), 'body': 'hi'});
/// // ^ already queued; no SyncTableRef needed.
/// ```
///
/// Outbox bookkeeping rows (`_sync_outbox`, `_sync_state`) never re-enter
/// the queue. Pull-side applies in [DbSync] write through the same [Db],
/// so they WOULD be re-queued by this adapter — to avoid push loops,
/// [DbSync] is not enough alone here: prefer the [SyncTableRef] wrapper as
/// the primary API, or route pull writes through the inner adapter. This
/// adapter exposes [runUntracked] for exactly that.
///
/// For most apps the simpler `sync.table()` wrapper is enough; use this
/// adapter only when retrofitting an existing codebase with scattered
/// `db.table()` writes.
class SyncTrackingAdapter implements DbAdapter {
  /// Wrapped storage backend.
  final DbAdapter inner;

  /// Tables whose writes are queued for push.
  final Set<String> syncedTables;

  /// Primary-key column. Defaults to `'id'`.
  final String idColumn;

  /// Whether this adapter is currently bypassing tracking (see
  /// [runUntracked]). Managed as a re-entrancy counter so nested calls
  /// and transactions stay untracked until the outermost scope exits.
  int _bypassDepth = 0;

  /// Creates a tracking decorator over [inner].
  SyncTrackingAdapter(
    this.inner, {
    required Set<String> syncedTables,
    this.idColumn = 'id',
  }) : syncedTables = Set.of(syncedTables);

  /// Whether tracking is currently bypassed.
  bool get isBypassed => _bypassDepth > 0;

  /// Runs [action] with tracking disabled (for pull-side applies and
  /// maintenance). Supports nesting.
  Future<T> runUntracked<T>(Future<T> Function() action) async {
    _bypassDepth++;
    try {
      return await action();
    } finally {
      _bypassDepth--;
    }
  }

  bool _track(String table) =>
      !isBypassed &&
      syncedTables.contains(table) &&
      table != kOutboxTable &&
      table != kStateTable;

  Future<void> _enqueue(
    DbAdapter tx,
    String table,
    String rowId,
    SyncOperation op, [
    Map<String, Object?>? data,
  ]) async {
    // Structured insert so both the sqlite and the pure-Dart fake backends
    // accept it (MemoryAdapter.execute only allows DDL + clears).
    // Outbox tables are excluded from tracking, so this never recurses,
    // and inside transactions [tx] is the tx adapter, keeping the write
    // atomic with the data change.
    final encoded =
        data == null ? null : jsonEncode(data, toEncodable: _encodable);
    await tx.insert(kOutboxTable, {
      'table_name': table,
      'row_id': rowId,
      'op': op.name,
      'data': encoded,
      'updated_at': nowIso(),
      'attempts': 0,
      'last_error': null,
    });
  }

  static Object? _encodable(Object? v) {
    if (v is DateTime) return v.toIso8601String();
    if (v is Enum) return v.name;
    return v.toString();
  }

  @override
  Future<int> insert(String table, Map<String, Object?> row) async {
    final id = await inner.insert(table, row);
    if (_track(table)) {
      final effectiveId = row[idColumn] ?? id;
      await _enqueue(inner, table, stringifyId(effectiveId),
          SyncOperation.upsert, {idColumn: effectiveId, ...row});
    }
    return id;
  }

  @override
  Future<void> insertMany(
      String table, List<Map<String, Object?>> rows) async {
    await inner.insertMany(table, rows);
    if (_track(table)) {
      for (final row in rows) {
        final id = row[idColumn];
        if (id != null) {
          await _enqueue(inner, table, stringifyId(id),
              SyncOperation.upsert, Map<String, Object?>.from(row));
        }
      }
    }
  }

  @override
  Future<void> upsert(String table, Map<String, Object?> row,
      {List<String> onConflict = const ['id']}) async {
    await inner.upsert(table, row, onConflict: onConflict);
    if (_track(table)) {
      final id = row[idColumn];
      if (id != null) {
        await _enqueue(inner, table, stringifyId(id),
            SyncOperation.upsert, Map<String, Object?>.from(row));
      }
    }
  }

  @override
  Future<int> update(
      String table, Map<String, Object?> values, [Condition? where]) async {
    List<Map<String, Object?>>? before;
    if (_track(table)) {
      final q = SelectQuery(table);
      if (where != null) q.whereCond(where);
      before = await inner.select(q);
    }
    final n = await inner.update(table, values, where);
    if (_track(table) && before != null) {
      for (final row in before) {
        final id = row[idColumn];
        if (id == null) continue;
        final fresh = await inner.select(
            SelectQuery(table).where((w) => w.eq(idColumn, id)));
        await _enqueue(inner, table, stringifyId(id),
            SyncOperation.upsert,
            fresh.isEmpty
                ? {idColumn: id, ...values}
                : fresh.first);
      }
    }
    return n;
  }

  @override
  Future<int> delete(String table, [Condition? where]) async {
    List<Map<String, Object?>>? before;
    if (_track(table)) {
      final q = SelectQuery(table);
      if (where != null) q.whereCond(where);
      before = await inner.select(q);
    }
    final n = await inner.delete(table, where);
    if (_track(table) && before != null) {
      for (final row in before) {
        final id = row[idColumn];
        if (id != null) {
          await _enqueue(
              inner, table, stringifyId(id), SyncOperation.delete);
        }
      }
    }
    return n;
  }

  @override
  Future<int> updateIncrement(
      String table, String column, num by, [Condition? where]) async {
    List<Map<String, Object?>>? before;
    if (_track(table)) {
      final q = SelectQuery(table);
      if (where != null) q.whereCond(where);
      before = await inner.select(q);
    }
    final n = await inner.updateIncrement(table, column, by, where);
    if (_track(table) && before != null) {
      for (final row in before) {
        final id = row[idColumn];
        if (id == null) continue;
        final fresh = await inner.select(
            SelectQuery(table).where((w) => w.eq(idColumn, id)));
        if (fresh.isNotEmpty) {
          await _enqueue(inner, table, stringifyId(id),
              SyncOperation.upsert, fresh.first);
        }
      }
    }
    return n;
  }

  @override
  Future<List<Map<String, Object?>>> select(SelectQuery query) =>
      inner.select(query);

  @override
  Future<List<Map<String, Object?>>> rawSelect(String sql,
          [List<Object?> args = const []]) =>
      inner.rawSelect(sql, args);

  @override
  Future<void> execute(String sql, [List<Object?> args = const []]) =>
      inner.execute(sql, args);

  @override
  Future<int> count(String table, [Condition? where]) =>
      inner.count(table, where);

  @override
  Future<bool> exists(String table, [Condition? where]) =>
      inner.exists(table, where);

  @override
  Future<T> transaction<T>(Future<T> Function(DbAdapter tx) action) async {
    return inner.transaction((tx) async {
      final tracked = SyncTrackingAdapter(tx,
          syncedTables: syncedTables, idColumn: idColumn);
      if (isBypassed) tracked._bypassDepth = 1;
      return action(tracked);
    });
  }

  @override
  Future<void> close() => inner.close();
}
