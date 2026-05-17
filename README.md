# otel_aws_kinesis

Dartastic OpenTelemetry bridge for AWS Kinesis Data Streams.

Wraps each call in a CLIENT-kind span with vendor-specific attributes following OpenTelemetry semantic conventions where applicable.

```dart
import 'package:otel_aws_kinesis/otel_aws_kinesis.dart';

await tracedAwsKinesis<void>(
  operation: 'someOp',
  invoke: () async {
    final r = await client.someOp();
    return AwsKinesisCallResult(value: r, result: AwsKinesisResult(statusCode: 200));
  },
);
```

Suppress instrumentation within a block:

```dart
await runWithoutAwsKinesisInstrumentationAsync(() async {
  await client.someInternalSync();
});
```
