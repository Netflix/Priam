package com.bazaarvoice.priam.client;

import com.bazaarvoice.ostrich.dropwizard.healthcheck.ContainsHealthyEndPointCheck;
import com.bazaarvoice.ostrich.pool.ServicePoolProxies;

/**
 * Dropwizard health check.
 */
public class PriamCassAdminHealthCheck extends ContainsHealthyEndPointCheck {
    public PriamCassAdminHealthCheck(PriamCassAdminClient priamCassAdminClient) {
        super(ServicePoolProxies.getPool(priamCassAdminClient), "priam");
    }
}
