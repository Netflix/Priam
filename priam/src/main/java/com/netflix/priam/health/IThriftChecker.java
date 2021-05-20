package com.netflix.priam.health;

import com.google.inject.ImplementedBy;

@ImplementedBy(ThriftChecker.class)
public interface IThriftChecker {
    boolean isThriftServerListening();
}
