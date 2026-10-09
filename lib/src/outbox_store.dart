/// Durable outbox + pull cursor stored inside the local dbkit database.
library;

import 'dart:convert';

import 'package:dbkit/dbkit.dart';

import 'sync_change.dart';

/// Local table holding queued (not yet pushed) changes.
const String kOutboxTable = '_sync_outbox';

/// Local table holding the last successful pull cursor per synced table.
const String kStateTable = '_sync_state';

/// Creates `_sync_outbox` / `_sync_state` when missing.
///
/// Safe to call repeatedly (`CREATE TABLE IF NOT EXISTS`). Works on both
/// the `sqlite3` and the pure-Dart fake backend (the fake treats DDL as a
/// no-op and stores rows in memory).
Future<void> ensureSyncSchema(Db db) async {
  await db.exec(
    'CREATE TABLE IF NOT EXISTS "$kOutboxTable" ('
    '"id" INTEGER PRIMARY KEY AUTOINCREMENT, '
    '"table_name" TEXT NOT NULL, '
    '"row_id" TEXT NOT NULL, '
    '"op" TEXT NOT NULL, '
    '"data" TEXT, '
    '"updated_at" TEXT NOT NULL, '
    '"attempts" INTEGER NOT NULL DEFAULT 0, '
    '"last_error" TEXT)',
  );
  await db.exec(
    'CREATE TABLE IF NOT EXISTS "$kStateTable" ('
    '"table_name" TEXT PRIMARY KEY, '
    '"last_pull_at" TEXT)',
  );
}

/// Durable FIFO queue of local changes awaiting push.
class OutboxStore {
  /// Database holding the outbox tables (see [ensureSyncSchema]).
  final Db db;

  /// Creates a store over [db].
  const OutboxStore(this.db);

  /// Enqueues one change. Returns the outbox row id.
  Future<int> enqueue({
    required String table,
    required String rowId,
    required SyncOperation op,
    Map<String, Object?>? data,
    DateTime? updatedAt,
  }) async {
    final at = (updatedAt ?? DateTime.now().toUtc()).toIso8601String();
    final encoded =
        data == null ? null : jsonEncode(data, toEncodable: _encodable);
    return db.table(kOutboxTable).insert({
      'table_name': table,
      'row_id': rowId,
      'op': op.name,
      'data': encoded,
      'updated_at': at,
      'attempts': 0,
      'last_error': null,
    });
  }

  /// Oldest-first pending entries, optionally capped at [limit].
  Future<List<OutboxEntry>> pending({int? limit}) async {
    var q = db.table(kOutboxTable).query().orderBy('id');
    if (limit != null) q = q.limit(limit);
    final rows = await q.get();
    return rows.map(_fromRow).toList();
  }

  /// Pending entries for one row (used for conflict detection).
  Future<List<OutboxEntry>> pendingForRow(String table, String rowId) async {
    final rows = await db
        .table(kOutboxTable)
        .where((w) => w.eq('table_name', table) & w.eq('row_id', rowId))
        .orderBy('id')
        .get();
    return rows.map(_fromRow).toList();
  }

  /// Number of queued entries.
  Future<int> count() => db.table(kOutboxTable).count();

  /// Deletes entries by outbox [ids] after a successful push.
  Future<void> deleteByIds(List<int> ids) async {
    if (ids.isEmpty) return;
    await db
        .table(kOutboxTable)
        .query()
        .where((w) => w.inList('id', ids))
        .get()
        .then((_) => db.table(kOutboxTable).deleteWhere((w) => w.inList('id', ids)));
  }

  /// Records a failed push attempt for [outboxId].
  Future<void> markFailed(int outboxId, Object error) async {
    final rows = await db
        .table(kOutboxTable)
        .where((w) => w.eq('id', outboxId))
        .get();
    if (rows.isEmpty) return;
    final attempts = ((rows.first['attempts'] as num?) ?? 0).toInt() + 1;
    await db.table(kOutboxTable).updateById(
        outboxId, {'attempts': attempts, 'last_error': '$error'});
  }

  /// Drops queued entries for one row (remote-wins resolution).
  Future<void> deleteForRow(String table, String rowId) async {
    await db.table(kOutboxTable).deleteWhere(
        (w) => w.eq('table_name', table) & w.eq('row_id', rowId));
  }

  /// Clears the whole queue.
  Future<void> clear() async {
    await db.table(kOutboxTable).truncate();
  }

  OutboxEntry _fromRow(Map<String, Object?> row) {
    final dataRaw = row['data'];
    Map<String, Object?>? data;
    if (dataRaw is String && dataRaw.isNotEmpty) {
      data = Map<String, Object?>.from(jsonDecode(dataRaw) as Map);
    }
    return OutboxEntry(
      outboxId: (row['id'] as num).toInt(),
      table: row['table_name'] as String,
      rowId: '${row['row_id']}',
      op: syncOperationFromString(row['op'] as String),
      data: data,
      updatedAt: DateTime.parse(row['updated_at'] as String),
      attempts: ((row['attempts'] as num?) ?? 0).toInt(),
      lastError: row['last_error'] as String?,
    );
  }

  static Object? _encodable(Object? v) {
    if (v is DateTime) return v.toIso8601String();
    if (v is Enum) return v.name;
    return v.toString();
  }
}

/// Pull cursor per table (`last_pull_at` ISO-8601).
class SyncStateStore {
  /// Database holding the state table (see [ensureSyncSchema]).
  final Db db;

  /// Creates a store over [db].
  const SyncStateStore(this.db);

  /// Last successful pull time for [table], or `null` before the first pull.
  Future<DateTime?> getLastPull(String table) async {
    final row = await db
        .table(kStateTable)
        .where((w) => w.eq('table_name', table))
        .first();
    final raw = row?['last_pull_at'] as String?;
    if (raw == null) return null;
    return DateTime.tryParse(raw);
  }

  /// Records a successful pull of [table] at [time] (defaults to now).
  Future<void> setLastPull(String table, [DateTime? time]) async {
    final at = (time ?? DateTime.now().toUtc()).toIso8601String();
    await db.table(kStateTable).upsert(
      {'table_name': table, 'last_pull_at': at},
      onConflict: ['table_name'],
    );
  }

  /// Clears all cursors (next pull is a full pull).
  Future<void> clear() async {
    await db.table(kStateTable).truncate();
  }
}

/// Stringifies a local primary key for outbox storage.
String stringifyId(Object? id) => '$id';
