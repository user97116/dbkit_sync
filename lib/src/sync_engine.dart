/// Offline-first sync engine: push outbox, pull remotes, resolve conflicts.
library;

import 'dart:async';

import 'package:dbkit/dbkit.dart';

import 'outbox_store.dart';
import 'sync_backend.dart';
import 'sync_change.dart';
import 'sync_config.dart';
import 'synced_table.dart';
import 'utils.dart';

/// Lifecycle state of a sync run (emitted on [DbSync.status]).
enum SyncStatus {
  /// No sync in progress.
  idle,

  /// A sync run started.
  syncing,

  /// Uploading local outbox entries.
  pushing,

  /// Downloading remote changes.
  pulling,

  /// The run completed.
  success,

  /// The run failed (see [SyncEvent.message]).
  failure,
}

/// A status broadcast emitted while syncing.
class SyncEvent {
  /// Current lifecycle state.
  final SyncStatus status;

  /// Human-readable detail (table name, counts, error, ...).
  final String? message;

  /// Creates a status event.
  const SyncEvent(this.status, [this.message]);

  @override
  String toString() =>
      'SyncEvent($status${message == null ? '' : ': $message'})';
}

/// Outcome of one [DbSync.sync] run.
class SyncResult {
  /// Outbox entries successfully pushed.
  final int pushed;

  /// Remote changes applied locally.
  final int pulled;

  /// Tables pulled (in order).
  final List<String> tables;

  /// Non-fatal notes (e.g. skipped conflicts); fatal errors throw instead.
  final List<String> notes;

  /// Creates a sync outcome.
  const SyncResult({
    this.pushed = 0,
    this.pulled = 0,
    this.tables = const [],
    this.notes = const [],
  });

  @override
  String toString() => 'SyncResult(pushed=$pushed, pulled=$pulled)';
}

/// Offline-first synchronizer between a local dbkit [Db] and a [SyncBackend].
///
/// Local writes made through [table()] are queued in `_sync_outbox`;
/// [sync()] pulls remote changes first (resolving conflicts against queued
/// local edits), then pushes the queue. Pull-first ordering means a stale
/// local edit never blindly overwrites a newer remote row on a dumb
/// last-write store.
///
/// ```dart
/// final db = Db.memory();
/// await db.createTable('notes', (t) {
///   t.text('id').primary();
///   t.text('body').nullable();
///   t.text('updated_at').nullable();
/// });
///
/// final sync = await DbSync.init(
///   db: db,
///   backend: MemorySyncBackend(),
///   tables: ['notes'],
/// );
/// await sync.table('notes').insert({'id': newSyncId(), 'body': 'hi'});
/// await sync.sync();
/// ```
class DbSync {
  /// Local database (reads + pull-side writes go here directly).
  final Db db;

  /// Remote target (Firebase / Supabase / REST / custom).
  final SyncBackend backend;

  /// What to sync and how.
  final SyncConfig config;

  /// Durable outbox queue.
  final OutboxStore outbox;

  /// Pull cursors.
  final SyncStateStore state;

  final StreamController<SyncEvent> _events =
      StreamController<SyncEvent>.broadcast();

  bool _syncing = false;
  Timer? _autoTimer;

  /// Creates a synchronizer (prefers [DbSync.init] which creates schema).
  DbSync({
    required this.db,
    required this.backend,
    required this.config,
    OutboxStore? outbox,
    SyncStateStore? state,
  })  : outbox = outbox ?? OutboxStore(db),
        state = state ?? SyncStateStore(db) {
    config.validate();
  }

  /// Creates schema (`_sync_outbox` / `_sync_state`) then returns a [DbSync].
  static Future<DbSync> init({
    required Db db,
    required SyncBackend backend,
    required List<String> tables,
    SyncDirection direction = SyncDirection.bidirectional,
    ConflictPolicy policy = ConflictPolicy.lastWriteWins,
    ConflictResolver? resolver,
    String idColumn = 'id',
    String updatedAtField = 'updated_at',
    bool touchUpdatedAt = false,
    int batchSize = 200,
  }) async {
    final config = SyncConfig(
      tables: tables,
      direction: direction,
      policy: policy,
      resolver: resolver,
      idColumn: idColumn,
      updatedAtField: updatedAtField,
      touchUpdatedAt: touchUpdatedAt,
      batchSize: batchSize,
    );
    config.validate();
    await ensureSyncSchema(db);
    return DbSync(db: db, backend: backend, config: config);
  }

  /// Broadcast of lifecycle events (see [SyncEvent]).
  Stream<SyncEvent> get status => _events.stream;

  /// Whether a sync run is currently in progress.
  bool get isSyncing => _syncing;

  /// Whether [table] is covered by [config].
  bool isSynced(String table) => config.tables.contains(table);

  /// Tracked handle to [table]. Throws when [table] is not configured.
  SyncTableRef table(String table) {
    if (!isSynced(table)) {
      throw ArgumentError.value(
          table, 'table', 'not in sync tables: ${config.tables}');
    }
    return SyncTableRef(
      db,
      table,
      outbox: outbox,
      idColumn: config.idColumn,
      updatedAtField: config.updatedAtField,
      touchUpdatedAt: config.touchUpdatedAt,
    );
  }

  /// Manually records an externally written row (for code that writes via
  /// `db.table()` directly instead of [table()]).
  Future<void> recordExternalUpsert(
      String table, Map<String, Object?> row) async {
    final id = row[config.idColumn];
    if (id == null) return;
    await outbox.enqueue(
      table: table,
      rowId: stringifyId(id),
      op: SyncOperation.upsert,
      data: Map<String, Object?>.from(row),
    );
  }

  /// Manually records an externally deleted row id.
  Future<void> recordExternalDelete(String table, Object id) async {
    await outbox.enqueue(
      table: table,
      rowId: stringifyId(id),
      op: SyncOperation.delete,
    );
  }

  /// Queued (not yet pushed) change count.
  Future<int> pendingCount() => outbox.count();

  /// Pulls remotes first, then pushes the outbox (per [config.direction]).
  ///
  /// Pull-first ordering keeps last-write-wins correct against dumb stores:
  /// a queued stale edit is resolved against the fresh remote *before*
  /// anything uploads, so it can never blindly overwrite a newer server
  /// row. Concurrent calls are serialized: a second caller awaits the
  /// in-flight run's outcome by polling. Throws the backend error on
  /// push/pull failure after emitting [SyncStatus.failure].
  Future<SyncResult> sync() async {
    if (_syncing) {
      // Serialize concurrent callers without overlapped cursor updates.
      while (_syncing) {
        await Future<void>.delayed(const Duration(milliseconds: 25));
      }
      return const SyncResult();
    }
    _syncing = true;
    _emit(SyncStatus.syncing, 'start');
    var pushed = 0;
    var pulled = 0;
    final tables = <String>[];
    try {
      if (config.direction != SyncDirection.pushOnly) {
        for (final t in config.tables) {
          _emit(SyncStatus.pulling, t);
          pulled += await pullTable(t);
          tables.add(t);
        }
      }
      if (config.direction != SyncDirection.pullOnly) {
        _emit(SyncStatus.pushing, 'push');
        pushed = await push();
      }
      _emit(SyncStatus.success, 'pushed=$pushed pulled=$pulled');
      return SyncResult(pushed: pushed, pulled: pulled, tables: tables);
    } catch (e) {
      _emit(SyncStatus.failure, '$e');
      rethrow;
    } finally {
      _syncing = false;
    }
  }

  /// Pushes queued outbox entries in [SyncConfig.batchSize] batches.
  /// Returns the pushed count.
  Future<int> push() async {
    var total = 0;
    while (true) {
      final batch = await outbox.pending(limit: config.batchSize);
      if (batch.isEmpty) return total;
      final changes =
          batch.map((e) => SyncChange(table: e.table, rowId: e.rowId, op: e.op, data: e.data, updatedAt: e.updatedAt)).toList();
      try {
        await backend.push(changes);
      } catch (e) {
        for (final entry in batch) {
          await outbox.markFailed(entry.outboxId, e);
        }
        rethrow;
      }
      await outbox.deleteByIds(batch.map((e) => e.outboxId).toList());
      total += batch.length;
    }
  }

  /// Pulls one [table] since its cursor and applies remote changes.
  /// Returns the applied count.
  Future<int> pullTable(String table) async {
    final since = await state.getLastPull(table);
    final remote = await backend.pull(
        SyncPullRequest(table: table, since: since, limit: config.batchSize));
    // Drain paged backends: keep pulling while full pages arrive.
    final all = List<SyncChange>.of(remote);
    while (remote.length >= config.batchSize && all.isNotEmpty) {
      final cursor = all.map((c) => c.updatedAt).reduce(
          (a, b) => a.isAfter(b) ? a : b);
      final next = await backend.pull(SyncPullRequest(
          table: table, since: cursor, limit: config.batchSize));
      if (next.isEmpty) break;
      all.addAll(next);
      if (next.length < config.batchSize) break;
    }
    all.sort((a, b) => a.updatedAt.compareTo(b.updatedAt));
    var applied = 0;
    for (final change in all) {
      if (await _applyRemote(change)) applied++;
    }
    final cursor = all.isEmpty
        ? DateTime.now().toUtc()
        : all.map((c) => c.updatedAt).reduce(
            (a, b) => a.isAfter(b) ? a : b);
    await state.setLastPull(table, cursor);
    return applied;
  }

  /// Applies one remote [change]. Returns true when the local db changed.
  Future<bool> _applyRemote(SyncChange change) async {
    if (change.table == kOutboxTable || change.table == kStateTable) {
      return false;
    }
    final ref = db.table(change.table);
    final pending = await outbox.pendingForRow(change.table, change.rowId);
    final local = await _findLocal(ref, change.rowId);

    if (pending.isNotEmpty && local != null) {
      switch (config.policy) {
        case ConflictPolicy.localWins:
          return false;
        case ConflictPolicy.remoteWins:
          await outbox.deleteForRow(change.table, change.rowId);
          break;
        case ConflictPolicy.custom:
          final resolver = config.resolver;
          if (resolver == null) return false;
          final merged = await resolver(
              table: change.table, local: local, remote: change);
          if (merged == null) return false;
          await _rawUpsert(change.table, merged);
          await outbox.deleteForRow(change.table, change.rowId);
          await outbox.enqueue(
            table: change.table,
            rowId: change.rowId,
            op: SyncOperation.upsert,
            data: Map<String, Object?>.from(merged),
          );
          return true;
        case ConflictPolicy.lastWriteWins:
          final localTime = _localTime(local, pending);
          final remoteTime = extractChangeTime(
              change.data, config.updatedAtField, change.updatedAt);
          if (localTime.isAfter(remoteTime)) return false;
          // Tie or remote newer -> remote wins.
          await outbox.deleteForRow(change.table, change.rowId);
          break;
      }
    } else if (pending.isNotEmpty && local == null) {
      // Local row deleted but outbox tombstone not yet pushed, or local
      // insert pending while remote deletes: fall through to policy below.
      if (config.policy == ConflictPolicy.localWins) return false;
      if (config.policy == ConflictPolicy.custom) {
        // No local row to merge; treat like remote-wins for deletes.
        if (change.op == SyncOperation.upsert) {
          // keep pending local tombstone vs remote upsert -> LWW compare
          final localTime = pending
              .map((e) => e.updatedAt)
              .reduce((a, b) => a.isAfter(b) ? a : b);
          final remoteTime = extractChangeTime(
              change.data, config.updatedAtField, change.updatedAt);
          if (localTime.isAfter(remoteTime)) return false;
        }
        await outbox.deleteForRow(change.table, change.rowId);
      } else {
        await outbox.deleteForRow(change.table, change.rowId);
      }
    }

    if (change.op == SyncOperation.delete) {
      final n = await _rawDelete(change.table, change.rowId);
      return n > 0;
    } else {
      final data = change.data;
      if (data == null || data.isEmpty) return false;
      await _rawUpsert(change.table, data);
      return true;
    }
  }

  DateTime _localTime(
      Map<String, Object?> local, List<OutboxEntry> pending) {
    var t = extractChangeTime(
        local, config.updatedAtField, DateTime.fromMillisecondsSinceEpoch(0, isUtc: true));
    for (final p in pending) {
      final pt = extractChangeTime(p.data, config.updatedAtField, p.updatedAt);
      if (pt.isAfter(t)) t = pt;
    }
    return t;
  }

  Future<Map<String, Object?>?> _findLocal(
      TableRef ref, String rowId) async {
    for (final variant in _idVariants(rowId)) {
      final row = await ref.findById(variant);
      if (row != null) return row;
    }
    return null;
  }

  List<Object> _idVariants(String rowId) {
    final parsed = int.tryParse(rowId);
    if (parsed != null) return [parsed, rowId];
    return [rowId];
  }

  Future<void> _rawUpsert(String table, Map<String, Object?> data) async {
    final row = Map<String, Object?>.from(data);
    // Preserve local int-PK typing when the server stringified the id.
    final id = row[config.idColumn];
    if (id is String) {
      final parsed = int.tryParse(id);
      if (parsed != null) {
        final existing = await _findLocal(db.table(table), id);
        final sample = existing?[config.idColumn];
        if (sample is int) row[config.idColumn] = parsed;
      }
    }
    if (!row.containsKey(config.idColumn) || row[config.idColumn] == null) {
      return;
    }
    await db.table(table).upsert(row, onConflict: [config.idColumn]);
  }

  Future<int> _rawDelete(String table, String rowId) async {
    var total = 0;
    for (final variant in _idVariants(rowId)) {
      total += await db.table(table).deleteById(variant);
      if (total > 0) break;
    }
    return total;
  }

  /// Starts periodic background sync every [interval].
  void startAutoSync(Duration interval) {
    stopAutoSync();
    _autoTimer = Timer.periodic(interval, (_) async {
      if (!_syncing) {
        try {
          await sync();
        } catch (_) {
          // Swallowed: failures stay queued and surface on status stream
          // plus the next manual sync().
        }
      }
    });
  }

  /// Stops periodic background sync started with [startAutoSync].
  void stopAutoSync() {
    _autoTimer?.cancel();
    _autoTimer = null;
  }

  /// Clears the outbox and all pull cursors (next pull is a full pull).
  Future<void> reset() async {
    await outbox.clear();
    await state.clear();
  }

  /// Stops timers, closes status stream and backend resources.
  Future<void> dispose() async {
    stopAutoSync();
    await backend.dispose();
    if (!_events.isClosed) await _events.close();
  }

  void _emit(SyncStatus status, [String? message]) {
    if (!_events.isClosed) _events.add(SyncEvent(status, message));
  }
}
