package org.jboss.pnc.logprocessor.eventduration;

import org.apache.kafka.streams.KafkaStreams;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Liveness;
import org.eclipse.microprofile.health.Readiness;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
@Liveness
@Readiness
@ApplicationScoped
public class StreamHealthCheck implements HealthCheck {

    @Inject
    private InContainerBoot inContainerBoot;

    @Override
    public HealthCheckResponse call() {
        KafkaStreams.State streamState = inContainerBoot.getStreamState();
        switch (streamState) {
            case RUNNING:
                return HealthCheckResponse.up("Stream is running.");

            default:
                return HealthCheckResponse.down("Stream state is: " + streamState.name());
        }
    }
}
