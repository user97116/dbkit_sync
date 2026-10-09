/// Supabase backend wired through user-supplied callbacks (no SDK dep).
library;

import '../sync_backend.dart';
import '../sync_change.dart';

/// Upserts rows into a Supabase table (by primary key).
typedef SupabaseUpsert = Future<void> Function(
    String table, List<Map<String, Object?>> rows);

/// Deletes rows from a Supabase table by id.
typedef SupabaseDelete = Future<void> Function(
    String table, List<String> ids);

/// Fetches rows changed after [since], ascending by time.
///
/// Each map must carry the primary key under [idField] and the
/// modification time under [updatedAtField]. Rows flagged with
/// `row[deletedField] == true` are pulled as deletes.
typedef SupabaseFetchSince = Future<List<Map<String, Object?>>> Function(
    String table, DateTime? since, int limit);

/// Syncs each dbkit table to a Supabase table of the same name.
///
/// This package does NOT depend on `supabase_flutter`: pass closures over
/// your `SupabaseClient`. Schema tip: give every synced table
/// `id text primary key`, `updated_at timestamptz default now()`, and a
/// `_deleted bool default false` soft-delete flag (hard deletes cannot be
/// pulled incrementally without a tombstone table).
///
/// ```dart
/// // Wire-up (in your app, with supabase_flutter imported):
/// SupabaseSyncBackend(
///   upsert: (table, rows) => supabase.from(table).upsert(rows),
///   deleteByIds: (table, ids) =>
///       supabase.from(table).delete().inFilter('id', ids),
///   fetchSince: (table, since, limit) async {
///     var q = supabase.from(table).select().order('updated_at').limit(limit);
///     if (since != null) {
///       q = q.gt('updated_at', since.toIso8601String());
///     }
///     final res = await q;
///     return (res as List).map((e) => Map<String, Object?>.from(e)).toList();
///   },
/// )
/// ```
class SupabaseSyncBackend implements SyncBackend {
  /// Upserts rows into the remote table.
  final SupabaseUpsert upsert;

  /// Deletes rows by id.
  final SupabaseDelete deleteByIds;

  /// Fetches rows changed since a cursor.
  final SupabaseFetchSince fetchSince;

  /// Primary-key key. Defaults to `'id'`.
  final String idField;

  /// Modification-time key. Defaults to `'updated_at'`.
  final String updatedAtField;

  /// Soft-delete flag pulled as [SyncOperation.delete].
  /// Defaults to `'_deleted'`. Set to `''` to disable.
  final String deletedField;

  /// Creates a Supabase backend from user-supplied client calls.
  const SupabaseSyncBackend({
    required this.upsert,
    required this.deleteByIds,
    required this.fetchSince,
    this.idField = 'id',
    this.updatedAtField = 'updated_at',
    this.deletedField = '_deleted',
  });

  @override
  Future<void> push(List<SyncChange> changes) async {
    final upserts = <String, List<Map<String, Object?>>>{};
    final deletes = <String, List<String>>{};
    for (final c in changes) {
      if (c.op == SyncOperation.delete) {
        deletes.putIfAbsent(c.table, () => []).add(c.rowId);
      } else {
        upserts
            .putIfAbsent(c.table, () => [])
            .add(Map<String, Object?>.from(c.data ?? {'id': c.rowId}));
      }
    }
    for (final e in upserts.entries) {
      await upsert(e.key, e.value);
    }
    for (final e in deletes.entries) {
      await deleteByIds(e.key, e.value);
    }
  }

  @override
  Future<List<SyncChange>> pull(SyncPullRequest request) async {
    final rows =
        await fetchSince(request.table, request.since, request.limit);
    final out = <SyncChange>[];
    for (final row in rows) {
      final id = '${row[idField]}';
      DateTime at = request.since ?? DateTime.fromMillisecondsSinceEpoch(0, isUtc: true);
      final raw = row[updatedAtField];
      if (raw is String) {
        at = DateTime.tryParse(raw) ?? DateTime.now().toUtc();
      } else if (raw is DateTime) {
        at = raw.toUtc();
      } else if (raw is int) {
        at = DateTime.fromMillisecondsSinceEpoch(raw, isUtc: true);
      }
      final isDeleted =
          deletedField.isNotEmpty && row[deletedField] == true;
      out.add(SyncChange(
        table: request.table,
        rowId: id,
        op: isDeleted ? SyncOperation.delete : SyncOperation.upsert,
        data: isDeleted ? null : Map<String, Object?>.from(row),
        updatedAt: at,
      ));
    }
    out.sort((a, b) => a.updatedAt.compareTo(b.updatedAt));
    return out;
  }

  @override
  Future<void> dispose() async {}
}
