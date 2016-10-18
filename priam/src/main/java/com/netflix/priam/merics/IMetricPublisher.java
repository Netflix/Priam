package com.netflix.priam.merics;

/**
 *
 * A means to publish Priam, Cassandra relaated metrics.
 * The default publisher is a noop.
 *
 * Created by vinhn on 10/14/16.
 */
public interface IMetricPublisher {
    public void publish(IMeasurement metric);
}
