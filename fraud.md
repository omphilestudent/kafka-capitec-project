# Fraud producer, consumer, topic/s and event/s

## Topic configs

```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic fraud-events \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=220924800000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=lz4 \
  --config max.message.bytes=1048576
```

### Consumer configs (fraud-scoring-consumer, reads payment-events)

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic payment-events \
  --group fraud-scoring-consumer \
  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --value-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --property print.key=true \
  --consumer-property max.poll.records=100 \
  --consumer-property session.timeout.ms=10000 \
  --consumer-property heartbeat.interval.ms=3000 \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property enable.auto.commit=false \
  --consumer-property auto.commit.interval.ms=5000 \
  --consumer-property max.poll.interval.ms=300000 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=50 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

### Producer configs

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic fraud-events \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --producer-property value.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --producer-property acks=all \
  --producer-property retries=2147483647 \
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=lz4 \
  --producer-property linger.ms=5 \
  --producer-property batch.size=32768 \
  --producer-property delivery.timeout.ms=30000 \
  --producer-property request.timeout.ms=10000
```

## Design rationale

**Topic**

- **Name `fraud-events`:** the fraud producer writes to its own topic, as the brief requires. It is kept separate from `payment-events` so fraud scores can be consumed without reading payment data.
- **Partitions `100`, replication factor `3`, `min.insync.replicas=2`:** the same reasoning as `payment-events`. The fraud score volume matches the payment volume, one score per payment, so the topic needs the same parallelism. See `payment.md` for the full derivation.
- **Retention `220924800000` ms (7 years):** the brief requires a 7-year hold for fraud events. Retention is time-based only, for the same compliance reason.
- **Cleanup policy `delete`, compression `lz4`, max message `1 MB`:** fraud score events are immutable and small, so the same settings apply.

**Producer (fraud score events)**

- **Event type:** `FRAUD_SCORE` with a `score` field of `LOW`, `MEDIUM` or `HIGH`, as the brief specifies. Each event also carries `payment_id`, `score_timestamp` and `model_version`.
- **Partition key `payment_id`:** the score for a payment lands on the same partition as that payment's events, so the two can be joined in order.
- **Durability and idempotency (`acks=all`, idempotence on):** a fraud score that is lost could let a fraudulent payment through unscored. The same settings as the payment producer apply.
- **Error handling:** a 30-second delivery timeout. If the score cannot be written in time, the payment is held for manual review rather than released unscored. Fail closed.

**Consumer (fraud-scoring-consumer)**

- **Purpose and lag SLA:** real-time fraud scoring of every `PAYMENT_INITIATED` event. Lag must stay under 50 ms, which drives the consumer tuning.
- **Event filtering:** it consumes all of `payment-events` and keeps only `PAYMENT_INITIATED`. Filtering happens in the consumer code, not in Kafka.
- **`max.poll.records=100`:** small batches keep the time from event to score under the 50 ms budget. Larger batches add queueing delay.
- **`fetch.max.wait.ms=50`:** the broker returns data within 50 ms even when the batch is not full, which matches the lag budget.
- **Offsets:** `enable.auto.commit=false`. The offset is committed only after a score is produced. If the consumer crashes, the event is re-scored, so scoring must be idempotent per `payment_id`.
- **`auto.offset.reset=earliest`:** the POC scores the mock events from the start. In production, a new fraud group should start from `latest` after cutover.
- **Failure detection:** a scoring error is retried a fixed number of times, then sent to a dead-letter topic and the payment is held for review.
- **Scalability:** up to 100 instances, one per partition. `session.timeout.ms=10000` with `heartbeat.interval.ms=3000` gives failover in about 10 seconds. The heartbeat stays under a third of the session timeout, as the client requires.
