package org.jboss.pnc.logprocessor.eventduration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.jboss.pnc.logprocessor.eventduration.domain.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * @author Ales Justin
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
class MergeTransformer implements Transformer<String, LogEvent, KeyValue<String, LogEvent>> {
    private static final Logger logger = LoggerFactory.getLogger(MergeTransformer.class);

    private KeyValueStore<String, LogEvent> store;

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        //noinspection unchecked
        store = (KeyValueStore<String, LogEvent>) context.getStateStore(LogProcessorTopology.LOG_STORE);
    }

    @Override
    public KeyValue<String, LogEvent> transform(String key, LogEvent thisLogEvent) {
        if (thisLogEvent.getEventType().isEmpty()) {
            //not an duration event
            return new KeyValue<>(key, thisLogEvent);
        }
        String identifier = thisLogEvent.getIdentifier();
        LogEvent firstLogEvent = store.delete(identifier); // get + remove
        if (firstLogEvent != null) {
            logger.debug("Found matching entry by identifier {}.", identifier);
            Duration duration = Duration.between(firstLogEvent.getTime(), thisLogEvent.getTime()).abs();
            if (firstLogEvent.getEventType().get().equals(LogEvent.EventType.BEGIN)) {
                //this is an END event
                thisLogEvent.addDuration(duration);
                return new KeyValue<>(key, thisLogEvent);
            } else {
                //this is a START event and the END event came in before the START event
                firstLogEvent.addDuration(duration);
                context.forward(key, thisLogEvent);
                return new KeyValue<>(firstLogEvent.getKafkaKey(), firstLogEvent);
            }
        } else {
            //this is a first event
            thisLogEvent.setKafkaKey(key);
            logger.debug("Storing entry with identifier {} and key {}.", identifier, key);
            store.put(identifier, thisLogEvent);
            if (thisLogEvent.getEventType().get().equals(LogEvent.EventType.BEGIN)) {
                return new KeyValue<>(key, thisLogEvent);
            } else {
                //the END event came first and it needs to be enriched with the duration
                //it must be forwarded when the START event gets in
                return null;
            }
        }
    }

    @Override
    public void close() {
    }
}
