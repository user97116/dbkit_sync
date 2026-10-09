/// Change models shared by the engine and all backends.
library;

/// Local or remote mutation for a single row.
enum SyncOperation {
  /// Insert or update the row (full document).
  upsert,

  /// Delete the row (tombstone).
  delete,
}

/// Parses a [SyncOperation] from its stored string form.
SyncOperation syncOperationFromString(String s) {
  switch (s) {
    case 'upsert':
      return SyncOperation.upsert;
    case 'delete':
      return SyncOperation.delete;
    default:
      throw ArgumentError.value(s, 's', 'unknown SyncOperation');
  }
}

/// A single row-level change.
///
/// [table] is the local table / remote collection name, [rowId] is the
/// stringified primary key (`row['id'].toString()`), [data] is the full
/// row for [SyncOperation.upsert] and `null` for deletes, and [updatedAt]
/// orders changes for last-write-wins resolution and incremental pull.
class SyncChange {
  /// Table / collection the row belongs to.
  final String table;

  /// Stringified primary key of the row.
  final String rowId;

  /// Whether this change writes or deletes the row.
  final SyncOperation op;

  /// Full row document for upserts, `null` for deletes.
  final Map<String, Object?>? data;

  /// Wall-clock time the change was produced.
  final DateTime updatedAt;

  /// Creates a row-level change.
  const SyncChange({
    required this.table,
    required this.rowId,
    required this.op,
    this.data,
    required this.updatedAt,
  });

  /// Serializes this change to a JSON-compatible map.
  Map<String, Object?> toJson() => {
        'table': table,
        'rowId': rowId,
        'op': op.name,
        'data': data,
        'updatedAt': updatedAt.toIso8601String(),
      };

  /// Deserializes a change produced by [toJson].
  factory SyncChange.fromJson(Map<String, Object?> json) {
    final data = json['data'];
    return SyncChange(
      table: json['table'] as String,
      rowId: '${json['rowId']}',
      op: syncOperationFromString(json['op'] as String),
      data: data == null ? null : Map<String, Object?>.from(data as Map),
      updatedAt: DateTime.parse(json['updatedAt'] as String),
    );
  }

  @override
  String toString() =>
      'SyncChange($table#$rowId ${op.name} @ $updatedAt)';
}

/// A locally queued change waiting to be pushed.
///
/// Extends [SyncChange] with the outbox row [outboxId], delivery [attempts],
/// and the last [lastError] for observability.
class OutboxEntry extends SyncChange {
  /// Primary key of the `_sync_outbox` row.
  final int outboxId;

  /// How many push attempts have failed so far.
  final int attempts;

  /// Failure message from the last attempt, if any.
  final String? lastError;

  /// Creates a queued local change.
  const OutboxEntry({
    required this.outboxId,
    required super.table,
    required super.rowId,
    required super.op,
    super.data,
    required super.updatedAt,
    this.attempts = 0,
    this.lastError,
  });

  @override
  String toString() =>
      'OutboxEntry(#$outboxId $table#$rowId ${op.name} attempts=$attempts)';
}
