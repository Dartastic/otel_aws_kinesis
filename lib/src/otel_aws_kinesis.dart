// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry_api/dartastic_opentelemetry_api.dart';

import 'otel_aws_kinesis_suppression.dart';

class AwsKinesisResult {
  const AwsKinesisResult({
    this.streamName,
    this.streamArn,
    this.shardId,
    this.sequenceNumber,
    this.partitionKey,
    this.recordCount,
    this.bytes,
    this.region,
    this.statusCode,
    this.errorCode,
    this.errorMessage,
  });

  final String? streamName;
  final String? streamArn;
  final String? shardId;
  final String? sequenceNumber;
  final String? partitionKey;
  final int? recordCount;
  final int? bytes;
  final String? region;
  final int? statusCode;
  final String? errorCode;
  final String? errorMessage;
}

class AwsKinesisCallResult<T> {
  const AwsKinesisCallResult({required this.value, this.result});
  final T value;
  final AwsKinesisResult? result;
}

Future<T> tracedAwsKinesis<T>({
  required String operation,
  required Future<AwsKinesisCallResult<T>> Function() invoke,
  Tracer? tracer,
}) async {
  if (awsKinesisInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final t = tracer ?? OTel.tracerProvider().getTracer('otel_aws_kinesis');
  final span = t.startSpan('kinesis $operation', kind: SpanKind.producer);
  try {
    span.setStringAttribute('messaging.system', 'aws_kinesis');
    final r = await invoke();
    final res = r.result;
    if (res != null) {
      if (res.streamName != null) {
        span.setStringAttribute('messaging.destination.name', res.streamName!);
      }
      if (res.streamArn != null) {
        span.setStringAttribute('aws.kinesis.stream.arn', res.streamArn!);
      }
      if (res.shardId != null) {
        span.setStringAttribute('aws.kinesis.shard.id', res.shardId!);
      }
      if (res.sequenceNumber != null) {
        span.setStringAttribute(
          'aws.kinesis.sequence_number',
          res.sequenceNumber!,
        );
      }
      if (res.partitionKey != null) {
        span.setStringAttribute('messaging.message.id', res.partitionKey!);
      }
      if (res.recordCount != null) {
        span.setIntAttribute('messaging.batch.message_count', res.recordCount!);
      }
      if (res.bytes != null) {
        span.setIntAttribute('messaging.message.body.size', res.bytes!);
      }
      if (res.region != null) {
        span.setStringAttribute('cloud.region', res.region!);
      }
      if (res.statusCode != null) {
        span.setIntAttribute('http.response.status_code', res.statusCode!);
      }
      if (res.errorCode != null) {
        span.setStringAttribute('kinesis.error.code', res.errorCode!);
      }
      final fail =
          res.errorCode != null ||
          (res.statusCode != null && res.statusCode! >= 400);
      if (fail) {
        span.setStringAttribute(
          ErrorResource.errorType.key,
          res.errorCode ?? 'kinesis.error',
        );
        span.setStatus(SpanStatusCode.Error, res.errorMessage);
      }
    }
    return r.value;
  } catch (e, st) {
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
