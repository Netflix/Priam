package com.netflix.priam.identity.token;

import com.google.inject.ImplementedBy;
import com.netflix.priam.identity.PriamInstance;
import java.util.Optional;

/** Fetches PriamInstances and other data which is convenient at the time */
@ImplementedBy(TokenRetriever.class)
public interface ITokenRetriever {
    PriamInstance get() throws Exception;
    /** Gets the IP address of the dead instance to which we will acquire its token */
    Optional<String> getReplacedIp();

    boolean isTokenPregenerated();
}
