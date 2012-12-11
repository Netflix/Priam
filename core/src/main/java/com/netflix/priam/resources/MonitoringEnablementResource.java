package com.netflix.priam.resources;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.dropwizard.managers.ServiceMonitorManager;
import com.yammer.dropwizard.jersey.params.BooleanParam;
import com.yammer.metrics.annotation.Timed;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Singleton
@Path("/v1/monitoringenabled")
@Produces (MediaType.APPLICATION_JSON)
public class MonitoringEnablementResource {
    @Inject private ServiceMonitorManager monitoringManager;

    @GET
    @Timed
    public Map<String, Object> isEnabled() {
        return ImmutableMap.<String, Object>of("enabled", monitoringManager.isRegistered());
    }

    @POST
    @Path ("/{state}")
    @Timed
    public Map<String, Object> setState(@PathParam ("state") BooleanParam state) {
        checkNotNull(state, "state");

        if (state.get()) {
            monitoringManager.register();
        } else {
            monitoringManager.deregister();
        }

        return ImmutableMap.<String, Object>of("enabled", monitoringManager.isRegistered());
    }
}
