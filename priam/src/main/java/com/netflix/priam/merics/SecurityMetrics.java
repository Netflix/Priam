package com.netflix.priam.merics;

import com.google.inject.Inject;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import javax.inject.Singleton;

/** Metrics pertaining to network security. Currently just publishes a count of ingress rules. */
@Singleton
public class SecurityMetrics {
    private final Gauge ingressRules;

    @Inject
    public SecurityMetrics(Registry registry) {
        ingressRules = registry.gauge(Metrics.METRIC_PREFIX + "ingress.rules");
    }

    public void setIngressRules(int count) {
        ingressRules.set(count);
    }
}
