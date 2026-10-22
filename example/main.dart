// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_aws_kinesis/otel_aws_kinesis.dart';

/// Traces Aws Kinesis calls with OpenTelemetry.
///
/// `otel_aws_kinesis` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Aws Kinesis call in
/// [tracedAwsKinesis]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'aws-kinesis-example');

  // Traces a Aws Kinesis call.
  final awsKinesisId = await tracedAwsKinesis<String>(
    operation: 'create',
    invoke: () async {
      // Make the Aws Kinesis call here, then report what it returned.
      const id = 'aws_kinesis_8f3a2c';
      return const AwsKinesisCallResult(
          value: id,
          result: AwsKinesisResult(
              streamName: 'example',
              streamArn: 'example',
              shardId: 'shard_8f3a2c',
              sequenceNumber: 'example',
              partitionKey: 'orders/2026/inv-1042.pdf',
              recordCount: 3,
              bytes: 512,
              region: 'us-east-1',
              statusCode: 201,
              errorCode: 'example',
              errorMessage: 'example'));
    },
  );
  OTelLog.info('tracedAwsKinesis create returned $awsKinesisId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutAwsKinesisInstrumentationAsync.
  await runWithoutAwsKinesisInstrumentationAsync(
    () => tracedAwsKinesis<String>(
      operation: 'create',
      invoke: () async {
        const id = 'aws_kinesis_8f3a2c';
        return const AwsKinesisCallResult(
            value: id,
            result: AwsKinesisResult(
                streamName: 'example',
                streamArn: 'example',
                shardId: 'shard_8f3a2c',
                sequenceNumber: 'example',
                partitionKey: 'orders/2026/inv-1042.pdf',
                recordCount: 3,
                bytes: 512,
                region: 'us-east-1',
                statusCode: 201,
                errorCode: 'example',
                errorMessage: 'example'));
      },
    ),
  );

  await OTel.shutdown();
}
