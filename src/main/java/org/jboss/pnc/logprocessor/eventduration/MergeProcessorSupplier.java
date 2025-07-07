package org.jboss.pnc.logprocessor.eventduration;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.jboss.pnc.logprocessor.eventduration.domain.LogEvent;

public class MergeProcessorSupplier implements ProcessorSupplier<String, LogEvent, String, LogEvent> {

    @Override
    public Processor<String, LogEvent, String, LogEvent> get() {
        return new MergeProcessor();
    }
}
