package org.jboss.pnc.logprocessor.eventduration;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import org.jboss.logging.Logger;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.io.IOException;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class Lifecycle {

    private final Logger logger = Logger.getLogger(Lifecycle.class);

    @Inject
    InContainerBoot application;

    public void start(@Observes StartupEvent event) throws IOException {
        logger.info("Starting application ...");
        application.init();
    }

    public void stop(@Observes ShutdownEvent event) {
        logger.info("Stopping application ...");
        application.destroy();
    }
}
