package com.netflix.priam.identity.token;

import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.IsNot.not;

import com.google.common.collect.ImmutableSet;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.utils.SystemUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import mockit.Expectations;
import mockit.Mocked;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class TokenRetrieverUtilsTest {
    private static final String APP = "testapp";
    private static final String STATUS_URL_FORMAT = "http://%s:8080/Priam/REST/v1/cassadmin/status";

    private ImmutableSet<PriamInstance> instances =
            ImmutableSet.copyOf(
                    IntStream.range(0, 6)
                            .<PriamInstance>mapToObj(
                                    e ->
                                            newMockPriamInstance(
                                                    APP,
                                                    "us-east",
                                                    (e < 3) ? "az1" : "az2",
                                                    e,
                                                    String.format("fakeInstance-%d", e),
                                                    String.format("127.0.0.%d", e),
                                                    String.format("fakeHost-%d", e),
                                                    String.valueOf(e)))
                            .collect(Collectors.toList()));

    private Map<String, String> tokenToEndpointMap =
            IntStream.range(0, 6)
                    .mapToObj(e -> Integer.valueOf(e))
                    .collect(
                            Collectors.toMap(
                                    e -> String.valueOf(e), e -> String.format("127.0.0.%s", e)));
    private List<String> liveInstances =
            IntStream.range(0, 6)
                    .mapToObj(e -> String.format("127.0.0.%d", e))
                    .collect(Collectors.toList());

    @Test
    public void testRetrieveTokenOwnerWhenGossipAgrees(@Mocked SystemUtils systemUtils) {
        // mark previous instance with tokenNumber 4 as down in gossip.
        List<String> myliveInstances =
                liveInstances
                        .stream()
                        .filter(x -> !x.equalsIgnoreCase("127.0.0.4"))
                        .collect(Collectors.toList());

        new Expectations() {
            {
                SystemUtils.getDataFromUrl(anyString);
                result = getStatus(myliveInstances, tokenToEndpointMap);
            }
        };

        TokenRetrieverUtils.InferredTokenOwnership inferredTokenOwnership =
                TokenRetrieverUtils.inferTokenOwnerFromGossip(instances, "4", "us-east");
        Assert.assertEquals(
                "127.0.0.4", inferredTokenOwnership.getTokenInformation().getIpAddress());
    }

    @Test
    public void testRetrieveTokenOwnerWhenGossipDisagrees(@Mocked SystemUtils systemUtils) {

        List<String> myliveInstances =
                liveInstances
                        .stream()
                        .filter(x -> !x.equalsIgnoreCase("127.0.0.4"))
                        .collect(Collectors.toList());

        new Expectations() {
            {
                SystemUtils.getDataFromUrl(
                        withArgThat(
                                allOf(
                                        not(String.format(STATUS_URL_FORMAT, "127.0.0.0")),
                                        not(String.format(STATUS_URL_FORMAT, "127.0.0.2")),
                                        not(String.format(STATUS_URL_FORMAT, "127.0.0.5")))));
                result = getStatus(myliveInstances, tokenToEndpointMap);
                minTimes = 0;

                SystemUtils.getDataFromUrl(String.format(STATUS_URL_FORMAT, "127.0.0.0"));
                result = getStatus(liveInstances, tokenToEndpointMap);
                minTimes = 0;

                SystemUtils.getDataFromUrl(String.format(STATUS_URL_FORMAT, "127.0.0.2"));
                result = getStatus(liveInstances, tokenToEndpointMap);
                minTimes = 0;

                SystemUtils.getDataFromUrl(String.format(STATUS_URL_FORMAT, "127.0.0.5"));
                result = null;
                minTimes = 0;
            }
        };

        TokenRetrieverUtils.InferredTokenOwnership inferredTokenOwnership =
                TokenRetrieverUtils.inferTokenOwnerFromGossip(instances, "4", "us-east");
        Assert.assertEquals(
                TokenRetrieverUtils.InferredTokenOwnership.TokenInformationStatus.MISMATCH,
                inferredTokenOwnership.getTokenInformationStatus());
        Assert.assertTrue(inferredTokenOwnership.getTokenInformation().isLive());
    }

    @Test
    public void testRetrieveTokenOwnerWhenGossipDisagrees_2Nodes(@Mocked SystemUtils systemUtils) {
        ImmutableSet<PriamInstance> myInstances =
                ImmutableSet.copyOf(instances.stream().limit(3).collect(Collectors.toList()));
        List<String> myLiveInstances = liveInstances.stream().limit(3).collect(Collectors.toList());
        Map<String, String> myTokenToEndpointMap =
                IntStream.range(0, 3)
                        .mapToObj(String::valueOf)
                        .collect(
                                Collectors.toMap(
                                        Function.identity(), (i) -> tokenToEndpointMap.get(i)));
        Map<String, String> alteredMap = new HashMap<>(myTokenToEndpointMap);
        alteredMap.put("1", "1.2.3.4");

        new Expectations() {
            {
                SystemUtils.getDataFromUrl(String.format(STATUS_URL_FORMAT, "127.0.0.0"));
                result = getStatus(myLiveInstances, myTokenToEndpointMap);
                minTimes = 0;

                SystemUtils.getDataFromUrl(String.format(STATUS_URL_FORMAT, "127.0.0.2"));
                result = getStatus(myLiveInstances, alteredMap);
                minTimes = 0;
            }
        };

        TokenRetrieverUtils.InferredTokenOwnership inferredTokenOwnership =
                TokenRetrieverUtils.inferTokenOwnerFromGossip(myInstances, "1", "us-east");
        Assert.assertEquals(
                TokenRetrieverUtils.InferredTokenOwnership.TokenInformationStatus.MISMATCH,
                inferredTokenOwnership.getTokenInformationStatus());
        Assert.assertTrue(inferredTokenOwnership.getTokenInformation().isLive());
    }

    @Test
    public void testRetrieveTokenOwnerWhenAllHostsInGossipReturnsNull(
            @Mocked SystemUtils systemUtils) throws Exception {
        new Expectations() {
            {
                SystemUtils.getDataFromUrl(anyString);
                result = getStatus(liveInstances, tokenToEndpointMap);
            }
        };

        TokenRetrieverUtils.InferredTokenOwnership inferredTokenOwnership =
                TokenRetrieverUtils.inferTokenOwnerFromGossip(instances, "4", "us-east");
        Assert.assertEquals(
                TokenRetrieverUtils.InferredTokenOwnership.TokenInformationStatus.GOOD,
                inferredTokenOwnership.getTokenInformationStatus());
        Assert.assertTrue(inferredTokenOwnership.getTokenInformation().isLive());
    }

    @Test
    public void testRetrieveTokenOwnerWhenAllInstancesThrowGossipParseException(
            @Mocked SystemUtils systemUtils) {

        new Expectations() {
            {
                SystemUtils.getDataFromUrl(anyString);
                result = new TokenRetrieverUtils.GossipParseException("Test");
            }
        };

        TokenRetrieverUtils.InferredTokenOwnership inferredTokenOwnership =
                TokenRetrieverUtils.inferTokenOwnerFromGossip(instances, "4", "us-east");
        Assert.assertEquals(
                TokenRetrieverUtils.InferredTokenOwnership.TokenInformationStatus.UNREACHABLE_NODES,
                inferredTokenOwnership.getTokenInformationStatus());
        Assert.assertNull(inferredTokenOwnership.getTokenInformation());
    }

    private String newGossipRecord(
            int tokenNumber, String ip, String dc, String rack, String status) {
        return String.format(
                "{\"TOKENS\":\"[%d]\",\"PUBLIC_IP\":\"%s\",\"RACK\":\"%s\",\"STATUS\":\"%s\",\"DC\":\"%s\"}",
                tokenNumber, ip, dc, status, rack);
    }

    private String getStatus(List<String> liveInstances, Map<String, String> tokenToEndpointMap) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("live", liveInstances);
            jsonObject.put("tokenToEndpointMap", tokenToEndpointMap);
        } catch (Exception e) {

        }
        return jsonObject.toString();
    }

    private PriamInstance newMockPriamInstance(
            String app,
            String dc,
            String rack,
            int id,
            String instanceId,
            String hostIp,
            String hostName,
            String token) {
        PriamInstance priamInstance = new PriamInstance();
        priamInstance.setApp(app);
        priamInstance.setDC(dc);
        priamInstance.setRac(rack);
        priamInstance.setId(id);
        priamInstance.setInstanceId(instanceId);
        priamInstance.setHost(hostName);
        priamInstance.setHostIP(hostIp);
        priamInstance.setToken(token);

        return priamInstance;
    }
}
