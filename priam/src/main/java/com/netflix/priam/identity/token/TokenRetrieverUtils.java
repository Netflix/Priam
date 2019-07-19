package com.netflix.priam.identity.token;

import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.utils.SystemUtils;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common utilities for token retrieval. */
public class TokenRetrieverUtils {
    private static final Logger logger = LoggerFactory.getLogger(TokenRetrieverBase.class);
    private static final String STATUS_URL_FORMAT = "http://%s:8080/Priam/REST/v1/cassadmin/status";

    /**
     * Utility method to infer the IP of the owner of a token in a given datacenter. This method
     * uses Cassandra gossip information to find the owner. While it is ideal to check all the nodes
     * in the ring to see if they agree on the IP to be replaced, in large clusters it may affect
     * the startup performance. This method picks at most 3 random hosts from the ring and see if
     * they all agree on the IP to be replaced. If not, it returns null.
     *
     * @param allIds
     * @param token
     * @param dc
     * @return IP of the token owner based on gossip information or null if gossip doesn't converge.
     * @throws GossipParseException when required number of instances are not available to fetch the
     *     gossip info.
     */
    public static String inferTokenOwnerFromGossip(
            List<? extends PriamInstance> allIds, String token, String dc)
            throws GossipParseException {

        // Avoid using dead instance who we are trying to replace (duh!!)
        // Avoid other regions instances to avoid communication over public ip address.
        List<? extends PriamInstance> eligibleInstances =
                allIds.stream()
                        .filter(priamInstance -> !priamInstance.getToken().equalsIgnoreCase(token))
                        .filter(priamInstance -> priamInstance.getDC().equalsIgnoreCase(dc))
                        .collect(Collectors.toList());
        // We want to get IP from min 1, max 3 instances to ensure we are not relying on
        // gossip of a single instance.
        // Good idea to shuffle so we are not talking to same instances every time.
        Collections.shuffle(eligibleInstances);
        // Potential issue could be when you have about 50% of your cluster C* DOWN or
        // trying to be replaced.
        // Think of a major disaster hitting your cluster. In that scenario chances of
        // instance hitting DOWN C* are much much higher.
        // In such a case you should rely on @link{CassandraConfig#setReplacedIp}.
        int noOfInstancesGossipShouldMatch = Math.max(1, Math.min(3, eligibleInstances.size()));

        // While it is ideal to check all the nodes in the ring to see if they agree on
        // the IP to be replaced, in large clusters it may affect the startup
        // performance. So we pick three random hosts from the ring and see if they all
        // agree on the IP to be replaced. If not, we don't replace.
        String replaceIp = null;
        int matchedGossipInstances = 0, reachableInstances = 0;
        for (PriamInstance instance : eligibleInstances) {
            logger.info(
                    "Calling getIp on hostname[{}] and token[{}]", instance.getHostName(), token);

            try {
                String ip = getIp(instance.getHostName(), token);
                reachableInstances++;

                if (StringUtils.isEmpty(ip)) {
                    continue;
                }

                if (replaceIp == null) {
                    replaceIp = ip;
                } else if (!replaceIp.equals(ip)) {
                    break;
                }

                matchedGossipInstances++;
                if (matchedGossipInstances == noOfInstancesGossipShouldMatch) {
                    return replaceIp;
                }
            } catch (GossipParseException e) {
                logger.warn(e.getMessage());
            }
        }

        // Throw exception if we are not able to reach at least minimum required
        // instances.
        if (reachableInstances < noOfInstancesGossipShouldMatch) {
            throw new GossipParseException(
                    "Unable to reach minimum required instances to fetch gossip information.");
        }

        logger.warn(
                "Return null: Unable to find enough instances where gossip match. Required: {}",
                noOfInstancesGossipShouldMatch);
        return null;
    }

    // helper method to get the token owner IP from a Cassandra node.
    private static String getIp(String host, String token) throws GossipParseException {
        String response = null;
        try {
            response = SystemUtils.getDataFromUrl(String.format(STATUS_URL_FORMAT, host));
            JSONObject jsonObject = (JSONObject) new JSONParser().parse(response);
            JSONArray liveNodes = (JSONArray) jsonObject.get("live");
            JSONObject tokenToEndpointMap = (JSONObject) jsonObject.get("tokenToEndpointMap");
            String endpointInfo = tokenToEndpointMap.get(token).toString();
            // We intentionally do not use the "unreachable" nodes as it may or may not be the best
            // place to start.
            // We just verify that the endpoint we provide is not "live".
            if (liveNodes.contains(endpointInfo)) return null;

            return endpointInfo;
        } catch (RuntimeException e) {
            throw new GossipParseException(
                    String.format("Error in reaching out to host: [{}]", host), e);
        } catch (ParseException e) {
            throw new GossipParseException(
                    String.format(
                            "Error in parsing gossip response [{}] from host: [{}]",
                            response,
                            host),
                    e);
        }
    }

    /**
     * This exception is thrown either when instances are not available or when they return invalid
     * response.
     */
    public static class GossipParseException extends Exception {
        private static final long serialVersionUID = 1462488371031437486L;

        public GossipParseException() {
            super();
        }

        public GossipParseException(String message) {
            super(message);
        }

        public GossipParseException(String message, Throwable t) {
            super(message, t);
        }
    }
}
