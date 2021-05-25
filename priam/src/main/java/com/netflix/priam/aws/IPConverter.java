package com.netflix.priam.aws;

import com.google.inject.ImplementedBy;
import com.netflix.priam.identity.PriamInstance;
import java.util.Optional;

/** Derives public ip from hostname. */
@ImplementedBy(AWSIPConverter.class)
public interface IPConverter {
    Optional<String> getPublicIP(PriamInstance instance);
}
