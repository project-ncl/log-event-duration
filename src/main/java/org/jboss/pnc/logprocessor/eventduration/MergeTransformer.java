package org.jboss.pnc.logprocessor.eventduration;

import io.micrometer.core.annotation.Timed;
import io.opentelemetry.instrumentation.annotations.SpanAttribute;
import io.opentelemetry.instrumentation.annotations.WithSpan;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jboss.pnc.logprocessor.eventduration.domain.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

/**
 * @author Ales Justin
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
class MergeTransformer implements Transformer<String, LogEvent, KeyValue<String, LogEvent>> {
    public static final String DEFAULT_KAFKA_MESSAGE_KEY = "0";
    private static final Logger logger = LoggerFactory.getLogger(MergeTransformer.class);

    private KeyValueStore<String, LogEvent> store;

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        // noinspection unchecked
        store = (KeyValueStore<String, LogEvent>) context.getStateStore(LogProcessorTopology.LOG_STORE);
    }

    @Timed
    @Override
    @WithSpan()
    public KeyValue<String, LogEvent> transform(
            @SpanAttribute(value = "key") String key,
            @SpanAttribute(value = "thisLogEvent") LogEvent thisLogEvent) {

        /*
         * This change is to support sending the output messages to multiple partitions.
         *
         * If the key is not set by the sender, set the key of the output to the mdc process context, if present.
         *
         * By default, Kafka uses the key of the message to send the message to a particular partition in the topic.
         * Since we care about message ordering for a specific build, and a specific build will have a specific process
         * context, all the logs for a specific build will be sent to the same specific partition, guaranteeing message
         * ordering.
         *
         * If the key is not set, we set the key to be that of the DEFAULT_KAFKA_MESSAGE_KEY. They'll all be sent to a
         * specific partition to maintain ordering of messages for that case.
         *
         * If the key remains null, Kafka's default behaviour is to load balance the message through the partitions of a
         * topic, which is not desirable in our case.
         */
        if (key == null) {
            key = thisLogEvent.getMdcProcessContext().orElse(DEFAULT_KAFKA_MESSAGE_KEY);
        }

        if (thisLogEvent == null) {
            return null;
        }
        if (thisLogEvent.getEventType().isEmpty()) {
            // not an duration event
            return new KeyValue<>(key, thisLogEvent);
        }
        String identifier = thisLogEvent.getIdentifier();
        LogEvent firstLogEvent = store.delete(identifier); // get + remove
        if (firstLogEvent != null) {
            logger.info("Found matching entry by identifier {}.", identifier);
            Duration duration = Duration.between(firstLogEvent.getTime(), thisLogEvent.getTime()).abs();
            if (firstLogEvent.getEventType().get().equals(LogEvent.EventType.BEGIN)) {
                if (thisLogEvent.getEventType().get().equals(LogEvent.EventType.END)) {
                    // this is an END event
                    thisLogEvent.addDuration(duration);
                } else {
                    logger.warn(
                            "Expected END log event but received identifier: {}, Type:{}.",
                            thisLogEvent.getIdentifier(),
                            thisLogEvent.getEventType());
                }
                return new KeyValue<>(key, thisLogEvent);
            } else {
                if (thisLogEvent.getEventType().get().equals(LogEvent.EventType.BEGIN)) {
                    // this is a START event and the END event came in before the START event
                    firstLogEvent.addDuration(duration);
                } else {
                    logger.warn(
                            "Expected BEGIN log event but received identifier: {}, Type:{}.",
                            thisLogEvent.getIdentifier(),
                            thisLogEvent.getEventType());
                }
                /*
                 * We use context.forward when we want to send more than 1 message to downstream queue in this transform
                 * method. The timestamp of the sent message is inherited from the input record.
                 *
                 * This causes an issue if we are consuming old messages. We cannot then send that message with the old
                 * timestamp to the downstream queue because Kafka doesn't like that the timestamp is so old.
                 *
                 * We can override this context.forward behaviour by using To.all().withTimestamp(latest timestamp);
                 */
                context.forward(key, thisLogEvent, To.all().withTimestamp(Instant.now().getEpochSecond()));
                return new KeyValue<>(firstLogEvent.getKafkaKey(), firstLogEvent);
            }
        } else {
            // this is a first event
            thisLogEvent.setKafkaKey(key);
            logger.info("Storing entry with identifier {} and key {}.", identifier, key);
            store.put(identifier, thisLogEvent);
            if (thisLogEvent.getEventType().get().equals(LogEvent.EventType.BEGIN)) {
                return new KeyValue<>(key, thisLogEvent);
            } else {
                // the END event came first and it needs to be enriched with the duration
                // it must be forwarded when the START event gets in
                return null;
            }
        }
    }

    @Override
    public void close() {
    }
}
