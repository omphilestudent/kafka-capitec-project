# Notification producer, consumer, topic/s and event/s

## Topic configs

```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic notification-events \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=lz4 \
  --config max.message.bytes=1048576
```

### Consumer configs (notification-sender-consumer, reads payment-events)

```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic payment-events \
  --group notification-sender-consumer \
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
  --consumer-property fetch.max.wait.ms=100 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

### Producer configs

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic notification-events \
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

- **Name `notification-events`:** the notification producer writes to its own topic, as the brief requires.
- **Partitions `100`, replication factor `3`, `min.insync.replicas=2`:** the notification consumer reads most of the payment stream, since it handles validated and failed payments. It needs the same parallelism, so the reasoning matches `payment.md`.
- **Retention `604800000` ms (7 days):** the brief sets the 7-year hold for payment and fraud events only. Notifications are customer messages, and the 7-day window gives enough room to replay after an outage. It is my choice, not a brief requirement. Confirm it with the business owner.
- **Cleanup policy `delete`, compression `lz4`, max message `1 MB`:** notification events are immutable and small.

**Producer (notification events)**

- **Event types:** `NOTIFICATION_PAYMENT_APPROVED` and `NOTIFICATION_PAYMENT_FAILED`, as the brief specifies. Each event carries `payment_id`, `customer_id`, `channel` and `template_id`.
- **Partition key `payment_id`:** all notifications for a payment stay in order. A "failed" message never overtakes an "approved" message for the same payment.
- **Durability (`acks=all`, idempotence on):** a missing notification leaves a customer unaware of a payment result. Duplicates are prevented, so a retry does not send two SMS messages.

**Consumer (notification-sender-consumer)**

- **Purpose and lag SLA:** sends the customer notification for each `PAYMENT_VALIDATED` and `PAYMENT_NOT_COMPLETED` event. Lag must stay under 2 seconds.
- **Event filtering:** it consumes `payment-events` and keeps only `PAYMENT_VALIDATED` and `PAYMENT_NOT_COMPLETED`. Filtering happens in the consumer code.
- **Offsets:** `enable.auto.commit=false`. The offset is committed only after the notification is handed to the sending channel. A crash can resend a message, so each send carries a `payment_id` plus event type key that the channel can deduplicate on.
- **`fetch.max.wait.ms=100`:** keeps delivery well inside the 2-second SLA while not spinning when the topic is idle.
- **`auto.offset.reset=earliest`:** the POC sends for the mock events from the start. In production, a new group should start from `latest` after cutover, so customers do not get old messages.
- **Failure detection:** a failed send is retried with backoff, then routed to a dead-letter topic. The customer is not silently skipped.
- **Scalability:** up to 100 instances, one per partition. Session and heartbeat values match the other consumers.
