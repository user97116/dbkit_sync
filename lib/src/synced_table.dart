/// Tracked write handle: use this instead of `db.table()` for synced tables.
library;

import 'package:dbkit/dbkit.dart';

import 'outbox_store.dart';
import 'sync_change.dart';
import 'utils.dart';

/// Write-tracking wrapper around a [TableRef].
///
/// Reads delegate straight through; every write additionally enqueues an
/// [OutboxEntry] so `DbSync` can push it later. Pull-side applies bypass
/// this wrapper (they write via the raw `Db`) so remote changes never
/// re-enter the outbox.
///
/// ```dart
/// final sync = await DbSync.init(db: db, backend: backend, tables: ['users']);
/// final users = sync.table('users');
/// await users.insert({'id': newSyncId(), 'name': 'Ada'});
/// await sync.sync(); // pushes the insert, then pulls remote changes
/// ```
class SyncTableRef {
  /// Local database.
  final Db db;

  /// Synced table name.
  final String table;

  /// Primary-key column. Defaults to `'id'`.
  final String idColumn;

  /// Timestamp field touched when [touchUpdatedAt] is true.
  final String updatedAtField;

  /// Whether writes auto-stamp `row[updatedAtField] = now`.
  /// Only enable when the table has that column.
  final bool touchUpdatedAt;

  final OutboxStore _outbox;

  /// Creates a tracked handle to [table].
  SyncTableRef(
    this.db,
    this.table, {
    required OutboxStore outbox,
    this.idColumn = 'id',
    this.updatedAtField = 'updated_at',
    this.touchUpdatedAt = false,
  }) : _outbox = outbox;

  TableRef get _ref => db.table(table);

  Map<String, Object?> _touch(Map<String, Object?> row) {
    if (!touchUpdatedAt) return row;
    if (row.containsKey(updatedAtField)) return row;
    return {...row, updatedAtField: nowIso()};
  }

  Future<void> _enqueueUpsert(Map<String, Object?> fullRow) async {
    final id = fullRow[idColumn];
    if (id == null) return;
    await _outbox.enqueue(
      table: table,
      rowId: stringifyId(id),
      op: SyncOperation.upsert,
      data: Map<String, Object?>.from(fullRow),
    );
  }

  Future<void> _enqueueDelete(Object id) async {
    await _outbox.enqueue(
      table: table,
      rowId: stringifyId(id),
      op: SyncOperation.delete,
    );
  }

  // -- writes (tracked) ------------------------------------------------------

  /// Inserts [row] and enqueues an upsert. Returns the new row id.
  Future<int> insert(Map<String, Object?> row) async {
    final touched = _touch(row);
    final id = await _ref.insert(touched);
    final full = {idColumn: touched[idColumn] ?? id, ...touched};
    // Object ids (string PKs) come back as 0 from sqlite; prefer the
    // caller-supplied id when present.
    final effectiveId = touched[idColumn] ?? id;
    await _outbox.enqueue(
      table: table,
      rowId: stringifyId(effectiveId),
      op: SyncOperation.upsert,
      data: Map<String, Object?>.from(full),
    );
    return id;
  }

  /// Inserts every row in [rows], enqueueing one upsert per row.
  Future<void> insertMany(List<Map<String, Object?>> rows) async {
    for (final row in rows) {
      await insert(row);
    }
  }

  /// Inserts or updates [row] on [onConflict] columns, enqueueing an upsert.
  Future<void> upsert(
    Map<String, Object?> row, {
    List<String> onConflict = const ['id'],
  }) async {
    final touched = _touch(row);
    if (touched[idColumn] == null && !(touched.containsKey(idColumn))) {
      await insert(touched);
      return;
    }
    await _ref.upsert(touched, onConflict: onConflict);
    final id = touched[idColumn];
    if (id != null) {
      await _outbox.enqueue(
        table: table,
        rowId: stringifyId(id),
        op: SyncOperation.upsert,
        data: Map<String, Object?>.from(touched),
      );
    }
  }

  /// Inserts [row] and returns it with its id. Enqueues a single upsert.
  Future<Map<String, Object?>> create(Map<String, Object?> row) async {
    final touched = _touch(row);
    final id = await _ref.insert(touched);
    final effectiveId = touched[idColumn] ?? id;
    final full = {idColumn: effectiveId, ...touched};
    // insert() above is bypassed here to avoid a double outbox entry.
    await _outbox.enqueue(
      table: table,
      rowId: stringifyId(effectiveId),
      op: SyncOperation.upsert,
      data: Map<String, Object?>.from(full),
    );
    return full;
  }

  /// Updates row [id] to [values] and enqueues an upsert with the fresh row.
  Future<int> updateById(Object id, Map<String, Object?> values) async {
    final touched = _touch(values);
    final n = await _ref.updateById(id, touched);
    final fresh =
        await _ref.findById(id) ?? {idColumn: id, ...touched};
    await _enqueueUpsert(fresh);
    return n;
  }

  /// Updates every row matching [build] and enqueues one upsert per row.
  Future<int> updateWhere(
    Condition Function(Where w) build,
    Map<String, Object?> values,
  ) async {
    final before = await _ref.findWhere(build);
    if (before.isEmpty) return 0;
    final touched = _touch(values);
    final n = await _ref.updateWhere(build, touched);
    for (final row in before) {
      final id = row[idColumn];
      if (id == null) continue;
      final fresh = await _ref.findById(id) ?? {idColumn: id, ...touched};
      await _enqueueUpsert(fresh);
    }
    return n;
  }

  /// Atomically adds [by] to [column] on matching rows; enqueues upserts.
  Future<int> increment(
    String column, {
    int by = 1,
    Condition Function(Where w)? where,
  }) async {
    final List<Map<String, Object?>> before;
    if (where == null) {
      before = await _ref.selectAll();
    } else {
      before = await _ref.findWhere(where);
    }
    if (before.isEmpty) return 0;
    final n = await _ref.increment(column, by: by, where: where);
    for (final row in before) {
      final id = row[idColumn];
      if (id == null) continue;
      final fresh = await _ref.findById(id);
      if (fresh != null) await _enqueueUpsert(fresh);
    }
    return n;
  }

  /// Atomically subtracts [by] from [column]; enqueues upserts.
  Future<int> decrement(
    String column, {
    int by = 1,
    Condition Function(Where w)? where,
  }) =>
      increment(column, by: -by, where: where);

  /// Deletes row [id] and enqueues a delete tombstone.
  Future<int> deleteById(Object id) async {
    final n = await _ref.deleteById(id);
    if (n > 0) await _enqueueDelete(id);
    return n;
  }

  /// Deletes every row matching [build], enqueueing one tombstone per row.
  Future<int> deleteWhere(Condition Function(Where w) build) async {
    final before = await _ref.findWhere(build);
    if (before.isEmpty) return 0;
    final n = await _ref.deleteWhere(build);
    for (final row in before) {
      final id = row[idColumn];
      if (id != null) await _enqueueDelete(id);
    }
    return n;
  }

  /// Deletes every row, enqueueing one tombstone per row.
  Future<int> truncate() async {
    final before = await _ref.selectAll();
    final n = await _ref.truncate();
    for (final row in before) {
      final id = row[idColumn];
      if (id != null) await _enqueueDelete(id);
    }
    return n;
  }

  // -- reads (passthrough, never tracked) ------------------------------------

  /// Starts a chainable read query (same as `db.table(name).query()`).
  TableQuery query() => _ref.query();

  /// Chainable filter shortcut (read-only).
  TableQuery where(Condition Function(Where w) build) => _ref.where(build);

  /// `SELECT * FROM table`.
  Future<List<Map<String, Object?>>> selectAll({
    String? orderBy,
    bool desc = false,
    int? limit,
    int? offset,
  }) =>
      _ref.selectAll(orderBy: orderBy, desc: desc, limit: limit, offset: offset);

  /// Row by id, or `null` when missing.
  Future<Map<String, Object?>?> findById(Object id) => _ref.findById(id);

  /// Row by id, throwing [DbException] when missing.
  Future<Map<String, Object?>> findByIdOrFail(Object id) =>
      _ref.findByIdOrFail(id);

  /// First row matching [build], or `null`.
  Future<Map<String, Object?>?> findOneWhere(
          Condition Function(Where w) build) =>
      _ref.findOneWhere(build);

  /// All rows matching [build].
  Future<List<Map<String, Object?>>> findWhere(
          Condition Function(Where w) build) =>
      _ref.findWhere(build);

  /// Row count, optionally filtered.
  Future<int> count([Condition Function(Where w)? build]) =>
      _ref.count(build);

  /// Whether any row matches [build].
  Future<bool> existsWhere(Condition Function(Where w) build) =>
      _ref.existsWhere(build);

  /// Single-column projection across matching rows.
  Future<List<T>> pluck<T>(String column,
          {Condition Function(Where w)? where}) =>
      _ref.pluck<T>(column, where: where);

  /// Paginated read.
  Future<Page> paginate({required int page, required int perPage}) =>
      _ref.paginate(page: page, perPage: perPage);
}
