package org.jboss.pnc.logprocessor.eventduration;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/probe")
public class Probe {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String probe() {
        return "true";
    }
}