/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.resources;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.httpclient.HttpStatus;
import org.codehaus.jettison.json.JSONObject;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.PriamServer;
import com.netflix.priam.metrics.HecubaTask;
import com.netflix.priam.metrics.MetricsCollector;
import com.netflix.priam.scheduler.PriamScheduler;

@Path("/v1/metrics")
@Produces(MediaType.APPLICATION_JSON)
public class HecubaServlet
{
    private static final Logger logger = LoggerFactory.getLogger(HecubaServlet.class);
	public static final String WAS_NOT_SCHEDULED = "was not scheduled";
    
    private final HecubaTask task;
	private final MetricsCollector collector;
	private final PriamScheduler scheduler;

	
    @Inject
    public HecubaServlet(PriamServer priamServer, IConfiguration config, MetricsCollector collector, 
    						PriamScheduler scheduler, HecubaTask task) {
        this.collector = collector;
        this.scheduler = scheduler;
        this.task = task;
    }

    
    @GET
    @Path("/status")
    public Response getStatus() throws Exception {
        
        JSONObject response = new JSONObject();
        response.put("metrics", stringifyMetricsState());
        response.put("successful executions", task.getExecutionCount());
        response.put("non-successful executions", task.getErrorCount());
        response.put("repeat", "every " + HecubaTask.INTERVAL +" ms");
        return Response.ok(response.toString(), MediaType.APPLICATION_JSON).build();
        
    }

    
    @GET
    @Path("/enable")
    public Response enableMetrics() throws Exception {
        
    	task.setMetricsEnabled(true);
    	JSONObject response = new JSONObject().put("metrics", stringifyMetricsState());
        return Response.ok(response.toString(), MediaType.APPLICATION_JSON).build();
        
    }

	@GET
    @Path("/disable")
    public Response disableMetrics() throws Exception {
        
    	task.setMetricsEnabled(false);
    	JSONObject response = new JSONObject().put("metrics", stringifyMetricsState());
        return Response.ok(response.toString(), MediaType.APPLICATION_JSON).build();
        
    }

    
    @GET
    @Path("/get")
    public Response getMetrics() throws Exception {
        
        JSONObject response = new JSONObject();
        List<MetricDatum> res = collector.collectMetrics();
        response.put("hecuba metrics", res);
        return Response.ok(response.toString(), MediaType.APPLICATION_JSON).build();
        
    }
    
    @GET
    @Path("/send")
    public Response sendMetrics() throws Exception {
        
        JSONObject response = new JSONObject();
        task.execute();
        response.put("sent metrics", HttpStatus.SC_OK);
        return Response.ok(response.toString(), MediaType.APPLICATION_JSON).build();
        
    }
	
	private String stringifyMetricsState() {
		return task.isMetricsEnabled() == true ? "enabled" : "disabled";
	}

}
