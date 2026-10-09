/// Small helpers: ids, clocks, id coercion.
library;

import 'dart:math';

final Random _random = Random.secure();

/// Generates a collision-resistant string id for sync-friendly tables.
///
/// Uses `DateTime.now()` microseconds + 64 random bits, hex-encoded.
/// Prefer `TEXT PRIMARY KEY` ids like this over `AUTOINCREMENT` ints when
/// multiple devices insert offline — integer sequences collide across
/// devices, string ids do not.
String newSyncId([DateTime? now]) {
  final t = (now ?? DateTime.now()).microsecondsSinceEpoch.toRadixString(16);
  final r = List.generate(16, (_) => _random.nextInt(256))
      .map((b) => b.toRadixString(16).padLeft(2, '0'))
      .join();
  return '${t}_$r';
}

/// Current UTC time as ISO-8601 (storage + wire format).
String nowIso() => DateTime.now().toUtc().toIso8601String();

/// Best-effort timestamp extraction for last-write-wins.
///
/// Prefers `row[updatedAtField]` when it parses as a [DateTime]/ISO string,
/// otherwise returns [fallback].
DateTime extractChangeTime(
  Map<String, Object?>? row,
  String updatedAtField,
  DateTime fallback,
) {
  final v = row?[updatedAtField];
  if (v is DateTime) return v.toUtc();
  if (v is String) {
    final parsed = DateTime.tryParse(v);
    if (parsed != null) return parsed.toUtc();
  }
  if (v is int) {
    // millis-since-epoch convention used by some backends.
    try {
      return DateTime.fromMillisecondsSinceEpoch(v, isUtc: true);
    } catch (_) {
      // fall through to fallback
    }
  }
  return fallback;
}

/// Coerces a stringified [rowId] back to the local id type.
///
/// Integer PKs come back from JSON as strings; comparing `'42'` with `42`
/// would miss. Returns an `int` when [rowId] parses and [sample] (or the
/// row data) suggests an int column, otherwise the original string.
Object coerceRowId(String rowId, {Object? sample}) {
  if (sample is int) {
    final parsed = int.tryParse(rowId);
    if (parsed != null) return parsed;
  }
  // Heuristic when no sample is available: pure digits -> int.
  if (sample == null && int.tryParse(rowId) != null) {
    return int.parse(rowId);
  }
  return rowId;
}
