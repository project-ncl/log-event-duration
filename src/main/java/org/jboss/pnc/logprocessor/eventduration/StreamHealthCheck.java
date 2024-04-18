package org.jboss.pnc.logprocessor.eventduration;

import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.eclipse.microprofile.health.Readiness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
@Liveness
@Readiness
@ApplicationScoped
public class StreamHealthCheck implements HealthCheck {

    private static final Logger log = LoggerFactory.getLogger(StreamHealthCheck.class);
    @Inject
    private InContainerBoot inContainerBoot;

    @Override
    public HealthCheckResponse call() {
        KafkaStreams.State streamState = inContainerBoot.getStreamState();
        switch (streamState) {
            case RUNNING:
                log.info("Health check requested with result: Stream is running.");
                return HealthCheckResponse.up("Stream is running.");

            default:
                log.info("Health check requested with result: {}" + streamState.name());
                return HealthCheckResponse.down("Stream state is: " + streamState.name());
        }
    }
}
