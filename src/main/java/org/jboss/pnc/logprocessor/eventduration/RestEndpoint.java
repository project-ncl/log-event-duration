package org.jboss.pnc.logprocessor.eventduration;

import java.time.ZonedDateTime;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.pnc.api.dto.ComponentVersion;

@Path("/")
public class RestEndpoint {

    @ConfigProperty(name = "quarkus.application.name")
    String name;

    @GET
    @Path("/version")
    @Produces(MediaType.APPLICATION_JSON)
    public ComponentVersion getVersion() {
        return ComponentVersion.builder()
                .name(name)
                .version(BuildInformationConstants.VERSION)
                .commit(BuildInformationConstants.COMMIT_HASH)
                .builtOn(ZonedDateTime.parse(BuildInformationConstants.BUILD_TIME))
                .build();
    }
}
