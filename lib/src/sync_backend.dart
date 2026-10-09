/// Backend contract every sync target must implement.
library;

import 'sync_change.dart';

/// Pull request for one table since the last successful pull.
class SyncPullRequest {
  /// Table / collection to pull.
  final String table;

  /// Only changes strictly newer than [since] should be returned.
  /// `null` means a full (initial) pull.
  final DateTime? since;

  /// Maximum rows the backend should return (best effort).
  final int limit;

  /// Creates a pull request for [table].
  const SyncPullRequest({required this.table, this.since, this.limit = 500});
}

/// Pluggable sync target: Firebase, Supabase, REST, or fully custom.
///
/// Implementations must be idempotent: pushing the same [SyncChange] twice
/// and pulling repeatedly with the same cursor must be safe. Push order is
/// the outbox order (oldest first); pull results should be ascending by
/// `updatedAt` so the engine can advance its cursor monotonically.
///
/// No Firebase/Supabase SDK dependency lives in this package: the bundled
/// `FirestoreSyncBackend` / `SupabaseSyncBackend` take plain callbacks so
/// you wire your own `cloud_firestore` / `supabase_flutter` calls.
abstract class SyncBackend {
  /// Pushes [changes] (oldest first). Should throw on failure so the
  /// engine keeps the outbox entries and retries later.
  Future<void> push(List<SyncChange> changes);

  /// Returns remote changes for [request.table] newer than
  /// `request.since`, ascending by `updatedAt`.
  Future<List<SyncChange>> pull(SyncPullRequest request);

  /// Releases backend resources (HTTP clients, subscriptions, ...).
  Future<void> dispose() async {}
}
