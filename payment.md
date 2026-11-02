# Payment producer, topic/s and event/s

## Topic configs

```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic payment-events \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=220924800000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=lz4 \
  --config max.message.bytes=1048576
```

### Consumer configs (payment-audit-consumer)

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic payment-events \
  --group payment-audit-consumer \
  --key-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --value-deserializer org.apache.kafka.common.serialization.StringDeserializer \
  --property print.key=true \
  --consumer-property max.poll.records=500 \
  --consumer-property session.timeout.ms=10000 \
  --consumer-property heartbeat.interval.ms=3000 \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property enable.auto.commit=false \
  --consumer-property auto.commit.interval.ms=5000 \
  --consumer-property max.poll.interval.ms=300000 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=500 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

### Producer configs

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic payment-events \
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

- **Name `payment-events`:** hyphen-separated and descriptive, as the brief asks. One topic holds every state transition of a payment, so consumers can filter by event type.
- **Partitions `100`:** the peak target is 1000 MB/s and one consumer instance handles 10 MB/s, so 1000 / 10 = 100 partitions gives the maximum parallelism for a consumer group. One producer instance handles 100 MB/s, so 1000 / 100 = 10 producer instances at peak. This assumes "Mb" means megabytes. If it means megabits, the peak is 125 MB/s and about 13 partitions would be enough.
- **Replication factor `3` and `min.insync.replicas=2`:** the cluster runs 3 brokers (`replicas: 3` in the statefulset) with `KAFKA_MIN_INSYNC_REPLICAS=2`. Any single broker can fail with no data loss and no write outage. Losing two brokers stops writes rather than losing payments.
- **Retention `220924800000` ms (2557 days):** this is the 7-year hold for payment events, with leap days included so the hold is never shorter than 7 years. Retention is time-based only. A size limit would delete events before the hold ends, which breaks compliance.
- **Cleanup policy `delete`:** payment events are immutable facts, and the topic is an event stream rather than a state store. Compaction would discard intermediate states, which the audit trail needs.
- **Compression `lz4`:** low CPU cost keeps the 150 ms payment latency budget safe. `zstd` gives a better ratio if storage cost becomes the constraint.
- **Max message size `1048576` (1 MB):** a payment event is about 1 to 2 KB, so 1 MB is generous headroom. It matches `max.partition.fetch.bytes` on the consumer.

**Producer**

- **Event types:** `PAYMENT_INITIATED`, `PAYMENT_AUTHORISED`, `PAYMENT_NOT_AUTHORISED`, `PAYMENT_VALIDATED`, `PAYMENT_INVALIDATED`, `PAYMENT_COMPLETED`, `PAYMENT_NOT_COMPLETED`, following the state machine in the brief.
- **Partition key `payment_id`:** every event for one payment lands on the same partition, in order. This gives the ordering guarantee that reconciliation and fraud need.
- **`acks=all`:** the producer waits until the leader and the in-sync replicas have the event. A payment is never reported as accepted if it could be lost.
- **`enable.idempotence=true`:** a network retry cannot write the same event twice. With `max.in.flight.requests.per.connection=5` (the maximum that keeps ordering safe with idempotence), per-partition order is preserved across retries.
- **Retries and timeouts:** `retries` is effectively unlimited and bounded by `delivery.timeout.ms=30000`. A payment event is at most stuck for 30 seconds before the producer reports failure. `request.timeout.ms=10000` is below the delivery timeout as the client requires. If the timeout is hit, the payment is retried by the caller or moved to a dead-letter path. It is never dropped silently.
- **Batching `linger.ms=5`, `batch.size=32768`:** a small batching window adds at most 5 ms of latency and keeps throughput high.
- **Serialization:** JSON strings, which suit the CLI POC. For production, Avro with a Schema Registry is better: it is compact, schema-checked, and evolves safely.
- **Mock volume:** 1,000 payment journeys, about 3 to 5 events each, with about 10% failed or not authorised to exercise error paths.

**Consumer (payment-audit-consumer)**

- **Purpose and lag SLA:** the 7-year audit and reconciliation archive. It needs every event. A lag under 1 hour is acceptable because it does not drive customer actions.
- **Offsets:** `enable.auto.commit=false`. Offsets are committed only after an event is written to the archive, which gives at-least-once delivery. Writes must be idempotent, deduplicated by `event_id`. `auto.commit.interval.ms` has no effect while auto-commit is off.
- **`auto.offset.reset=earliest`:** a new group reads from the start so the POC sees all mock events. Revisit this for production.
- **Assignment strategy `CooperativeStickyAssignor`:** rebalances move only the partitions that change owner, so the audit reader does not pause on every scale event.
- **Scalability:** up to 100 instances, one per partition.
