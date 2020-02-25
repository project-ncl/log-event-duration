package org.jboss.pnc.logprocessor.eventduration;

import io.quarkus.test.Mock;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;

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
