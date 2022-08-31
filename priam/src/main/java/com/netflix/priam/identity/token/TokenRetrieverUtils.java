package com.netflix.priam.identity.token;

import com.google.common.collect.ImmutableSet;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.utils.GsonJsonSerializer;
import com.netflix.priam.utils.SystemUtils;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common utilities for token retrieval. */
public class TokenRetrieverUtils {
    private static final Logger logger = LoggerFactory.getLogger(TokenRetrieverUtils.class);
    private static final String STATUS_URL_FORMAT = "http://%s:8080/Priam/REST/v1/cassadmin/status";

    /**
     * Utility method to infer the IP of the owner of a token in a given datacenter. This method
     * uses Cassandra status information to find the owner. While it is ideal to check all the nodes
     * in the ring to see if they agree on the IP to be replaced, in large clusters it may affect
     * the startup performance. This method picks at most 3 random hosts from the ring and see if
     * they all agree on the IP to be replaced. If not, it returns null.
     *
     * @param allIds
     * @param token
     * @param dc
     * @return IP of the token owner based on gossip information or null if C* status doesn't
     *     converge.
     */
    public static InferredTokenOwnership inferTokenOwnerFromGossip(
            ImmutableSet<PriamInstance> allIds, String token, String dc) {

        // Avoid using dead instance who we are trying to replace (duh!!)
        // Avoid other regions instances to avoid communication over public ip address.
        List<? extends PriamInstance> eligibleInstances =
                allIds.stream()
                        .filter(priamInstance -> !token.equalsIgnoreCase(priamInstance.getToken()))
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
        InferredTokenOwnership inferredTokenOwnership = new InferredTokenOwnership();
        int matchedGossipInstances = 0, reachableInstances = 0;
        for (PriamInstance instance : eligibleInstances) {
            logger.info("Finding down nodes from ip[{}]; token[{}]", instance.getHostIP(), token);

            try {
                TokenInformation tokenInformation =
                        getTokenInformation(instance.getHostIP(), token);
                reachableInstances++;

                if (inferredTokenOwnership.getTokenInformation() == null) {
                    inferredTokenOwnership.setTokenInformation(tokenInformation);
                }

                if (inferredTokenOwnership.getTokenInformation().equals(tokenInformation)) {
                    matchedGossipInstances++;
                    if (matchedGossipInstances == noOfInstancesGossipShouldMatch) {
                        inferredTokenOwnership.setTokenInformationStatus(
                                InferredTokenOwnership.TokenInformationStatus.GOOD);
                        return inferredTokenOwnership;
                    }
                } else {
                    // Mismatch in the gossip information from Cassandra.
                    inferredTokenOwnership.setTokenInformationStatus(
                            InferredTokenOwnership.TokenInformationStatus.MISMATCH);
                    logger.info(
                            "There is a mismatch in the status information reported by Cassandra. TokenInformation1: {}, TokenInformation2: {}",
                            inferredTokenOwnership.getTokenInformation(),
                            tokenInformation);
                    inferredTokenOwnership.setTokenInformation(
                            inferredTokenOwnership.getTokenInformation().isLive
                                    ? inferredTokenOwnership.getTokenInformation()
                                    : tokenInformation);
                    return inferredTokenOwnership;
                }

            } catch (GossipParseException e) {
                logger.warn(e.getMessage());
            }
        }

        // If we are not able to reach at least minimum required instances.
        if (reachableInstances < noOfInstancesGossipShouldMatch) {
            inferredTokenOwnership.setTokenInformationStatus(
                    InferredTokenOwnership.TokenInformationStatus.UNREACHABLE_NODES);
            logger.info(
                    String.format(
                            "Unable to find enough instances where gossip match. Required: [%d]",
                            noOfInstancesGossipShouldMatch));
        }

        return inferredTokenOwnership;
    }

    // helper method to get the token owner IP from a Cassandra node.
    private static TokenInformation getTokenInformation(String ip, String token)
            throws GossipParseException {
        String response = null;
        try {
            response = SystemUtils.getDataFromUrl(String.format(STATUS_URL_FORMAT, ip));
            JSONObject jsonObject = (JSONObject) new JSONParser().parse(response);
            JSONArray liveNodes = (JSONArray) jsonObject.get("live");
            JSONObject tokenToEndpointMap = (JSONObject) jsonObject.get("tokenToEndpointMap");
            String endpointInfo = tokenToEndpointMap.get(token).toString();
            // We intentionally do not use the "unreachable" nodes as it may or may not be the best
            // place to start.
            // We just verify that the endpoint we provide is not "live".
            boolean isLive = liveNodes.contains(endpointInfo);
            return new TokenInformation(endpointInfo, isLive);
        } catch (RuntimeException e) {
            throw new GossipParseException(
                    String.format("Error in reaching out to host: [%s]", ip), e);
        } catch (ParseException e) {
            throw new GossipParseException(
                    String.format(
                            "Error in parsing gossip response [%s] from host: [%s]", response, ip),
                    e);
        }
    }

    public static class TokenInformation {
        private String ipAddress;
        private boolean isLive;

        public TokenInformation(String ipAddress, boolean isLive) {
            this.ipAddress = ipAddress;
            this.isLive = isLive;
        }

        public boolean isLive() {
            return isLive;
        }

        public String getIpAddress() {
            return ipAddress;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || this.getClass() != obj.getClass()) return false;
            TokenInformation tokenInformation = (TokenInformation) obj;
            return this.ipAddress.equalsIgnoreCase(tokenInformation.getIpAddress())
                    && isLive == tokenInformation.isLive;
        }

        public String toString() {
            return GsonJsonSerializer.getGson().toJson(this);
        }
    }

    public static class InferredTokenOwnership {
        public enum TokenInformationStatus {
            GOOD,
            UNREACHABLE_NODES,
            MISMATCH
        }

        private TokenInformationStatus tokenInformationStatus =
                TokenInformationStatus.UNREACHABLE_NODES;
        private TokenInformation tokenInformation;

        public void setTokenInformationStatus(TokenInformationStatus tokenInformationStatus) {
            this.tokenInformationStatus = tokenInformationStatus;
        }

        public void setTokenInformation(TokenInformation tokenInformation) {
            this.tokenInformation = tokenInformation;
        }

        public TokenInformationStatus getTokenInformationStatus() {
            return tokenInformationStatus;
        }

        public TokenInformation getTokenInformation() {
            return tokenInformation;
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
