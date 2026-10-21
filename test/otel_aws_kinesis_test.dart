// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:otel_aws_kinesis/otel_aws_kinesis.dart';
import 'package:test/test.dart';

void main() {
  group('otel_aws_kinesis', () {
    test('suppression toggles in zone', () async {
      expect(awsKinesisInstrumentationSuppressed(), isFalse);
      await runWithoutAwsKinesisInstrumentationAsync(() async {
        expect(awsKinesisInstrumentationSuppressed(), isTrue);
      });
      expect(awsKinesisInstrumentationSuppressed(), isFalse);
    });

    test('AwsKinesisResult fields default null', () {
      const r = AwsKinesisResult();
      expect(r.statusCode, isNull);
    });
  });
}
