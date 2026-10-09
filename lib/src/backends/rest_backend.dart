/// Generic HTTP backend for any custom sync server.
library;

import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;

import '../sync_backend.dart';
import '../sync_change.dart';

/// Syncs against a custom HTTP server speaking this minimal protocol:
///
/// ```text
/// POST {baseUrl}/push   {"changes": [SyncChange.toJson(), ...]} -> 2xx
/// GET  {baseUrl}/pull?table=notes&since=<iso>&limit=500
///   -> {"changes": [SyncChange JSON, ...]}
/// ```
///
/// Built on `package:http`, so it works on every Dart and Flutter target
/// including the web. The server can be anything (Dart shelf, Node, Go,
/// Firebase Functions, Supabase Edge Functions). Extra [headers] (e.g.
/// `Authorization`) are sent on every request. Each request times out after
/// [timeout].
///
/// Pass your own [client] to share connection pools or to inject a
/// `MockClient` in tests; otherwise an internal client is created and
/// closed by [dispose].
class RestSyncBackend implements SyncBackend {
  /// Base URL of the sync server, e.g. `https://api.example.com/sync`.
  final String baseUrl;

  /// Extra headers sent on every request (auth tokens, tenant ids, ...).
  final Map<String, String> headers;

  /// Per-request timeout. Defaults to 15s.
  final Duration timeout;

  /// HTTP client. Defaults to an internal `http.Client()`.
  final http.Client client;

  final bool _ownsClient;

  /// Creates an HTTP backend targeting [baseUrl].
  RestSyncBackend({
    required this.baseUrl,
    this.headers = const {},
    this.timeout = const Duration(seconds: 15),
    http.Client? client,
  })  : client = client ?? http.Client(),
        _ownsClient = client == null;

  Uri _uri(String path, [Map<String, String>? query]) {
    final base = baseUrl.endsWith('/')
        ? baseUrl.substring(0, baseUrl.length - 1)
        : baseUrl;
    return Uri.parse('$base$path').replace(queryParameters: query);
  }

  Never _throwForStatus(String what, Uri uri, http.Response res) {
    throw http.ClientException(
        '$what failed: ${res.statusCode} ${res.body}', uri);
  }

  @override
  Future<void> push(List<SyncChange> changes) async {
    final uri = _uri('/push');
    final res = await client
        .post(uri,
            headers: {'content-type': 'application/json', ...headers},
            body: jsonEncode({
              'changes': changes.map((c) => c.toJson()).toList(),
            }))
        .timeout(timeout);
    if (res.statusCode < 200 || res.statusCode >= 300) {
      _throwForStatus('push', uri, res);
    }
  }

  @override
  Future<List<SyncChange>> pull(SyncPullRequest request) async {
    final uri = _uri('/pull', {
      'table': request.table,
      if (request.since != null)
        'since': request.since!.toUtc().toIso8601String(),
      'limit': '${request.limit}',
    });
    final res = await client.get(uri, headers: {...headers}).timeout(timeout);
    if (res.statusCode < 200 || res.statusCode >= 300) {
      _throwForStatus('pull', uri, res);
    }
    final decoded = jsonDecode(res.body) as Map<String, Object?>;
    final list = (decoded['changes'] as List? ?? const [])
        .cast<Map<String, Object?>>();
    final out = list.map((j) => SyncChange.fromJson(j)).toList()
      ..sort((a, b) => a.updatedAt.compareTo(b.updatedAt));
    return out;
  }

  @override
  Future<void> dispose() async {
    if (_ownsClient) client.close();
  }
}

/// In-process custom backend for tests and adapters that already speak
/// domain objects: implement [SyncBackend] directly, or wrap two closures.
class CustomSyncBackend implements SyncBackend {
  /// Push handler.
  final Future<void> Function(List<SyncChange> changes) onPush;

  /// Pull handler.
  final Future<List<SyncChange>> Function(SyncPullRequest request) onPull;

  /// Creates a backend from two closures.
  const CustomSyncBackend({required this.onPush, required this.onPull});

  @override
  Future<void> push(List<SyncChange> changes) => onPush(changes);

  @override
  Future<List<SyncChange>> pull(SyncPullRequest request) => onPull(request);

  @override
  Future<void> dispose() async {}
}
