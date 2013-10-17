package com.bazaarvoice.priam.client;

import java.util.List;
import java.util.Map;

/**
 * Interface for interacting with Priam's CassAdmin class
 * Note that not all CassAdmin's endpoint are available. Only the custom ones are.
 * This is due to the fact that the endpoints correspond to nodetool functions that are based on individual nodes, and
 * not the entire ring.
 */
public interface PriamCassAdmin {

    /**
     * Returns a list of hints info from each node in the entire ring.
     */
    List<Map<String, Object>> getHintsForTheRing();
}
