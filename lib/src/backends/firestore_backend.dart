/// Firestore backend wired through user-supplied callbacks (no SDK dep).
library;

import '../sync_backend.dart';
import '../sync_change.dart';

/// Reads one Firestore document by id, or `null` when missing.
typedef FirestoreRead = Future<Map<String, Object?>?> Function(
    String table, String id);

/// Writes (merges) one Firestore document.
typedef FirestoreWrite = Future<void> Function(
    String table, String id, Map<String, Object?> data);

/// Deletes one Firestore document.
typedef FirestoreDelete = Future<void> Function(String table, String id);

/// Lists documents in [table] changed after [since] (ascending by time).
///
/// Each returned map must include the document id under [idField] and its
/// modification time under [updatedAtField]. `null` [since] means a full
/// listing.
typedef FirestoreListSince = Future<List<Map<String, Object?>>> Function(
    String table, DateTime? since);

/// Syncs each dbkit table to a Firestore collection of the same name.
///
/// This package deliberately does NOT depend on `cloud_firestore`: pass
/// thin closures that call your `FirebaseFirestore` instance. Deleted rows
/// are propagated as document deletes; on pull, missing documents listed
/// with an `isDeleted` marker (or absent from a full listing) are NOT
/// auto-deleted — track deletes explicitly via [deletedField] or tombstone
/// collections for reliable delete propagation.
///
/// ```dart
/// // Wire-up (in your app, with cloud_firestore imported):
/// FirestoreSyncBackend(
///   write: (table, id, data) => FirebaseFirestore.instance
///       .collection(table).doc(id).set(data),
///   delete: (table, id) => FirebaseFirestore.instance
///       .collection(table).doc(id).delete(),
///   listSince: (table, since) async {
///     var q = FirebaseFirestore.instance.collection(table)
///         .orderBy('updated_at').limit(500);
///     if (since != null) {
///       q = q.where('updated_at', isGreaterThan: since.toIso8601String());
///     }
///     final snap = await q.get();
///     return snap.docs.map((d) => {'id': d.id, ...d.data()}).toList();
///   },
/// )
/// ```
class FirestoreSyncBackend implements SyncBackend {
  /// Writes/merges one document.
  final FirestoreWrite write;

  /// Deletes one document.
  final FirestoreDelete delete;

  /// Lists changed documents since a cursor.
  final FirestoreListSince listSince;

  /// Document-id key in maps returned by [listSince]. Defaults to `'id'`.
  final String idField;

  /// Modification-time key used for cursors. Defaults to `'updated_at'`.
  final String updatedAtField;

  /// Soft-delete marker: documents with `row[deletedField] == true` pull
  /// down as [SyncOperation.delete]. Defaults to `'_deleted'`.
  /// Set to `''` to disable soft-delete handling.
  final String deletedField;

  /// Creates a Firestore backend from user-supplied Firestore calls.
  const FirestoreSyncBackend({
    required this.write,
    required this.delete,
    required this.listSince,
    this.idField = 'id',
    this.updatedAtField = 'updated_at',
    this.deletedField = '_deleted',
  });

  @override
  Future<void> push(List<SyncChange> changes) async {
    for (final c in changes) {
      if (c.op == SyncOperation.delete) {
        await delete(c.table, c.rowId);
      } else {
        await write(c.table, c.rowId, Map<String, Object?>.from(c.data ?? {}));
      }
    }
  }

  @override
  Future<List<SyncChange>> pull(SyncPullRequest request) async {
    final rows = await listSince(request.table, request.since);
    final out = <SyncChange>[];
    for (final row in rows.take(request.limit)) {
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
