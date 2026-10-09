package org.jboss.pnc.logprocessor.eventduration;

import java.io.IOException;

import jakarta.enterprise.context.ApplicationScoped;

import io.quarkus.test.Mock;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
@ApplicationScoped
@Mock
public class InContainerBootMock extends InContainerBoot {

    @Override
    public void init() throws IOException {
        // NOOP
    }

    @Override
    public void destroy() {

    }
}
