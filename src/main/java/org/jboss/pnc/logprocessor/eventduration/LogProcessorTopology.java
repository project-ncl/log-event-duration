package org.jboss.pnc.logprocessor.eventduration;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.jboss.pnc.logprocessor.eventduration.domain.LogEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * @author Ales Justin
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class LogProcessorTopology {

    public static final String LOG_STORE = "log-store";

    private final String inputTopic;

    private final String outputTopic;

    private final Optional<String> durationsTopic;

    public LogProcessorTopology(String inputTopic, String outputTopic, Optional<String> durationsTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.durationsTopic = durationsTopic;
    }

    Topology buildTopology(Properties properties) {
        StreamsBuilder builder = new StreamsBuilder();

        Map<String, String> configuration = new HashMap<>();
        configuration.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        configuration.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0");
        configuration.put(TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(64 * 1024 * 1024));

        Serde<LogEvent> logSerde = Serdes.serdeFrom(new LogEvent.JsonSerializer(), new LogEvent.JsonDeserializer());
        KStream<String, LogEvent> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), logSerde));

        StoreBuilder<KeyValueStore<String, LogEvent>> storageStoreBuilder = Stores
                .keyValueStoreBuilder(Stores.inMemoryKeyValueStore(LOG_STORE), Serdes.String(), logSerde).withCachingEnabled()
                .withLoggingEnabled(configuration);
        builder.addStateStore(storageStoreBuilder);

        KStream<String, LogEvent> output = input.transform(MergeTransformer::new, LOG_STORE);

        output.to(outputTopic, Produced.with(Serdes.String(), logSerde));

        if (durationsTopic.isPresent()) {
            output.filter((key, logEvent) -> isEndLogEvent(logEvent)).to(durationsTopic.get(),
                    Produced.with(Serdes.String(), logSerde));
        }

        return builder.build(properties);
    }

    private boolean isEndLogEvent(LogEvent logEvent) {
        return logEvent.getEventType().isPresent() && logEvent.getEventType().get().equals(LogEvent.EventType.END);
    }

}
