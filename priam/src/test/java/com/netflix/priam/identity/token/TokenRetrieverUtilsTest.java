package com.netflix.priam.identity.token;

import static org.hamcrest.core.AllOf.allOf;
import static org.hamcrest.core.IsNot.not;

import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.utils.SystemUtils;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import junit.framework.Assert;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;

public class TokenRetrieverUtilsTest {
    private static final String APP = "testapp";
    private static final String STATUS_URL_FORMAT = "http://%s:8080/Priam/REST/v1/cassadmin/status";

    private List<PriamInstance> instances =
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
                    .collect(Collectors.toList());

    private String[] gossipInfos =
            IntStream.range(0, 6)
                    .<String>mapToObj(
                            e ->
                                    newGossipRecord(
                                            e,
                                            String.format("127.0.0.%d", e),
                                            "us-east-1",
                                            (e < 3) ? "az1" : "az2",
                                            "NORMAL"))
                    .collect(Collectors.toList())
                    .toArray(new String[0]);

    @Test
    public void testRetrieveTokenOwnerWhenGossipAgrees(@Mocked SystemUtils systemUtils)
            throws Exception {
        // updates instances with new instance owning token 4 as per token database.
        List<PriamInstance> newInstances =
                instances.stream().filter(e -> e.getId() != 4).collect(Collectors.toList());
        newInstances.add(
                newMockPriamInstance(
                        APP,
                        "us-east",
                        "az2",
                        4,
                        "fakeHost-400",
                        "127.0.0.400",
                        "fakeHost-400",
                        "4"));

        // mark previous instance with tokenNumber 4 as down in gossip.
        String[] newGossipInfos = Arrays.copyOf(gossipInfos, gossipInfos.length);
        newGossipInfos[4] = newGossipRecord(4, "127.0.0.4", "us-east-1", "az2", "shutdown");

        new Expectations() {
            {
                SystemUtils.getDataFromUrl(anyString);
                result = Arrays.toString(newGossipInfos);
            }
        };

        String replaceIp = TokenRetrieverUtils.inferTokenOwnerFromGossip(instances, "4", "us-east");
        Assert.assertEquals("127.0.0.4", replaceIp);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRetrieveTokenOwnerWhenGossipDisagrees(@Mocked SystemUtils systemUtils)
            throws Exception {
        // updates instances with new instance owning token 4 as per token database.
        List<PriamInstance> newInstances =
                instances.stream().filter(e -> e.getId() != 4).collect(Collectors.toList());

        // mark previous instance as token owner for tokenNumber 4 in the gossip.
        String[] gossipInfoSetOne = Arrays.copyOf(gossipInfos, gossipInfos.length);
        gossipInfoSetOne[4] = newGossipRecord(4, "127.0.0.4", "us-east-1", "az2", "shutdown");

        // mark empty as token owner for tokenNumber 4 in the gossip.
        String[] gossipInfoSetTwo = Arrays.copyOf(gossipInfos, gossipInfos.length);
        gossipInfoSetTwo[4] = newGossipRecord(4, "", "us-east-1", "az2", "shutdown");

        new Expectations() {
            {
                SystemUtils.getDataFromUrl(
                        withArgThat(
                                allOf(
                                        not(String.format(STATUS_URL_FORMAT, "fakeHost-0")),
                                        not(String.format(STATUS_URL_FORMAT, "fakeHost-5")))));
                result = Arrays.toString(gossipInfoSetTwo);

                SystemUtils.getDataFromUrl(String.format(STATUS_URL_FORMAT, "fakeHost-0"));
                result = Arrays.toString(gossipInfoSetOne);
                minTimes = 0;
                SystemUtils.getDataFromUrl(String.format(STATUS_URL_FORMAT, "fakeHost-5"));
                result = Arrays.toString(gossipInfoSetOne);
                minTimes = 0;
            }
        };

        String replaceIp =
                TokenRetrieverUtils.inferTokenOwnerFromGossip(newInstances, "4", "us-east");
        Assert.assertEquals(null, replaceIp);
    }

    @Test
    public void testRetrieveTokenOwnerWhenAllHostsInGossipReturnsNull(
            @Mocked SystemUtils systemUtils) throws Exception {
        // updates instances with new instance owning token 4 as per token database.
        List<PriamInstance> newInstances =
                instances.stream().filter(e -> e.getId() != 4).collect(Collectors.toList());
        newInstances.add(
                newMockPriamInstance(
                        APP,
                        "us-east",
                        "az2",
                        4,
                        "fakeInstance-400",
                        "127.0.0.400",
                        "fakeHost-400",
                        "4"));

        // mark empty as token owner for tokenNumber 4 in the gossip.
        String[] gossipInfo = Arrays.copyOf(gossipInfos, gossipInfos.length);
        gossipInfo[4] = newGossipRecord(4, "", "us-east-1", "az2", "shutdown");

        new Expectations() {
            {
                SystemUtils.getDataFromUrl(anyString);
                result = Arrays.toString(gossipInfo);
            }
        };

        String replaceIp = TokenRetrieverUtils.inferTokenOwnerFromGossip(instances, "4", "us-east");
        Assert.assertNull(replaceIp);
    }

    @Test(expected = TokenRetrieverUtils.GossipParseException.class)
    public void testRetrieveTokenOwnerWhenAllInstancesThrowGossipParseException(
            @Mocked SystemUtils systemUtils) throws TokenRetrieverUtils.GossipParseException {
        // updates instances with new instance owning token 4 as per token database.
        List<PriamInstance> newInstances =
                instances.stream().filter(e -> e.getId() != 4).collect(Collectors.toList());
        newInstances.add(
                newMockPriamInstance(
                        APP,
                        "us-east",
                        "az2",
                        4,
                        "fakeInstance-400",
                        "127.0.0.400",
                        "fakeHost-400",
                        "4"));

        new Expectations() {
            {
                SystemUtils.getDataFromUrl(anyString);
                result = new TokenRetrieverUtils.GossipParseException();
            }
        };

        String replaceIp = TokenRetrieverUtils.inferTokenOwnerFromGossip(instances, "4", "us-east");
        Assert.assertNull(replaceIp);
    }

    private String newGossipRecord(
            int tokenNumber, String ip, String dc, String rack, String status) {
        return String.format(
                "{\"TOKENS\":\"[%d]\",\"PUBLIC_IP\":\"%s\",\"RACK\":\"%s\",\"STATUS\":\"%s\",\"DC\":\"%s\"}",
                tokenNumber, ip, dc, status, rack);
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
