/// Sync configuration: what to sync and how to resolve conflicts.
library;

import 'sync_change.dart';

/// Push/pull direction for a sync run.
enum SyncDirection {
  /// Only upload local outbox entries.
  pushOnly,

  /// Only download remote changes.
  pullOnly,

  /// Push local changes first, then pull remote changes (default).
  bidirectional,
}

/// How to resolve a row that changed both locally and remotely.
enum ConflictPolicy {
  /// Newest `updatedAt` wins (ties go to remote). Default.
  lastWriteWins,

  /// Keep the local row, ignore the remote change.
  localWins,

  /// Apply the remote row, drop queued local changes for that row.
  remoteWins,

  /// Delegate to [SyncConfig.resolver].
  custom,
}

/// Merges a conflicting [local] row with its [remote] change.
///
/// Return the merged document to apply locally (and push on the next run),
/// or `null` to keep the local row as-is.
typedef ConflictResolver = Future<Map<String, Object?>?> Function({
  required String table,
  required Map<String, Object?> local,
  required SyncChange remote,
});

/// What to sync and how.
class SyncConfig {
  /// Tables / collections to synchronize.
  final List<String> tables;

  /// Push/pull direction. Defaults to [SyncDirection.bidirectional].
  final SyncDirection direction;

  /// Conflict handling. Defaults to [ConflictPolicy.lastWriteWins].
  final ConflictPolicy policy;

  /// Custom merger used when [policy] is [ConflictPolicy.custom].
  final ConflictResolver? resolver;

  /// Primary-key column of synced tables. Defaults to `'id'`.
  final String idColumn;

  /// Row timestamp field used for LWW when present in the row data,
  /// e.g. `'updated_at'`. Falls back to outbox/remote `updatedAt`
  /// when the column is absent.
  final String updatedAtField;

  /// When true (default `false`), tracked writes stamp
  /// `row[updatedAtField] = now` automatically. Only enable this when
  /// every synced table actually has that column, otherwise inserts fail
  /// with "no such column". Prefer explicit timestamps or leave `false`
  /// and rely on the outbox clock.
  final bool touchUpdatedAt;

  /// Max changes per push batch and per pull page. Defaults to 200.
  final int batchSize;

  /// Creates a sync configuration for [tables].
  const SyncConfig({
    required this.tables,
    this.direction = SyncDirection.bidirectional,
    this.policy = ConflictPolicy.lastWriteWins,
    this.resolver,
    this.idColumn = 'id',
    this.updatedAtField = 'updated_at',
    this.touchUpdatedAt = false,
    this.batchSize = 200,
  }) : assert(batchSize > 0, 'batchSize must be > 0');

  /// Validates the config, throwing [ArgumentError] on misuse.
  void validate() {
    if (tables.isEmpty) {
      throw ArgumentError.value(tables, 'tables', 'must not be empty');
    }
    if (policy == ConflictPolicy.custom && resolver == null) {
      throw ArgumentError.value(
          policy, 'policy', 'custom policy requires a resolver');
    }
    if (idColumn.trim().isEmpty) {
      throw ArgumentError.value(idColumn, 'idColumn', 'must not be empty');
    }
  }
}
