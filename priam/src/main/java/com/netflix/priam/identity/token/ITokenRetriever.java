package com.netflix.priam.identity.token;

import com.google.inject.ImplementedBy;
import com.netflix.priam.identity.PriamInstance;
import java.util.Optional;
import org.apache.commons.lang3.math.Fraction;

/** Fetches PriamInstances and other data which is convenient at the time */
@ImplementedBy(TokenRetriever.class)
public interface ITokenRetriever {
    PriamInstance get() throws Exception;

    /** Gets the IP address of the dead instance whose token we will acquire. */
    Optional<String> getReplacedIp();

    boolean isTokenPregenerated();

    /** returns the percentage of tokens in the ring which come before this node's token */
    Fraction getRingPosition() throws Exception;
}
