package com.netflix.priam.merics;

/**
 * A noop metric publisher.
 *
 * Created by vinhn on 10/14/16.
 */
public class NoOpMetricPublisher implements IMetricPublisher {
    @Override
    public void publish(IMeasurement o) {
        //No OP
    }
}
