/// Generic HTTP backend for any custom sync server.
library;

import 'dart:convert';
import 'dart:io';

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
/// The server can be anything (Dart shelf, Node, Go, Firebase Functions,
/// Supabase Edge Functions). Extra [headers] (e.g. `Authorization`) are
/// sent on every request. Times out after [timeout] per request.
class RestSyncBackend implements SyncBackend {
  /// Base URL of the sync server, e.g. `https://api.example.com/sync`.
  final String baseUrl;

  /// Extra headers sent on every request (auth tokens, tenant ids, ...).
  final Map<String, String> headers;

  /// Per-request timeout. Defaults to 15s.
  final Duration timeout;

  /// Creates an HTTP backend targeting [baseUrl].
  const RestSyncBackend({
    required this.baseUrl,
    this.headers = const {},
    this.timeout = const Duration(seconds: 15),
  });

  Uri _uri(String path, [Map<String, String>? query]) {
    final base = baseUrl.endsWith('/')
        ? baseUrl.substring(0, baseUrl.length - 1)
        : baseUrl;
    return Uri.parse('$base$path').replace(queryParameters: query);
  }

  @override
  Future<void> push(List<SyncChange> changes) async {
    final client = HttpClient();
    try {
      final req = await client
          .postUrl(_uri('/push'))
          .timeout(timeout);
      req.headers.contentType = ContentType.json;
      headers.forEach(req.headers.set);
      req.write(jsonEncode({
        'changes': changes.map((c) => c.toJson()).toList(),
      }));
      final res = await req.close().timeout(timeout);
      final body = await res.transform(utf8.decoder).join();
      if (res.statusCode < 200 || res.statusCode >= 300) {
        throw HttpException(
            'push failed: ${res.statusCode} $body', uri: req.uri);
      }
    } finally {
      client.close(force: true);
    }
  }

  @override
  Future<List<SyncChange>> pull(SyncPullRequest request) async {
    final client = HttpClient();
    try {
      final req = await client
          .getUrl(_uri('/pull', {
            'table': request.table,
            if (request.since != null)
              'since': request.since!.toUtc().toIso8601String(),
            'limit': '${request.limit}',
          }))
          .timeout(timeout);
      headers.forEach(req.headers.set);
      final res = await req.close().timeout(timeout);
      final body = await res.transform(utf8.decoder).join();
      if (res.statusCode < 200 || res.statusCode >= 300) {
        throw HttpException(
            'pull failed: ${res.statusCode} $body', uri: req.uri);
      }
      final decoded = jsonDecode(body) as Map<String, Object?>;
      final list = (decoded['changes'] as List? ?? const [])
          .cast<Map<String, Object?>>();
      final out =
          list.map((j) => SyncChange.fromJson(j)).toList()
            ..sort((a, b) => a.updatedAt.compareTo(b.updatedAt));
      return out;
    } finally {
      client.close(force: true);
    }
  }

  @override
  Future<void> dispose() async {}
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
