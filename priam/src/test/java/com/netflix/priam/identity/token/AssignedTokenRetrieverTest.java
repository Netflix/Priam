package com.netflix.priam.identity.token;

import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.IPriamInstanceFactory;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.identity.config.InstanceInfo;
import com.netflix.priam.utils.ITokenManager;
import com.netflix.priam.utils.Sleeper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class AssignedTokenRetrieverTest {
    public static final String APP = "testapp";
    public static final String DEAD_APP = "testapp-dead";

    @Test
    public void grabAssignedTokenStartDbInBootstrapModeWhenGossipAgreesCurrentInstanceIsTokenOwner(
            @Mocked IPriamInstanceFactory<PriamInstance> factory,
            @Mocked IConfiguration config,
            @Mocked IMembership membership,
            @Mocked Sleeper sleeper,
            @Mocked ITokenManager tokenManager,
            @Mocked InstanceInfo instanceInfo,
            @Mocked TokenRetrieverUtils retrievalUtils)
            throws Exception {
        List<PriamInstance> liveHosts = newPriamInstances();
        Collections.shuffle(liveHosts);

        new Expectations() {
            {
                config.getAppName();
                result = APP;

                factory.getAllIds(DEAD_APP);
                result = Collections.emptyList();

                factory.getAllIds(APP);
                result = liveHosts;

                instanceInfo.getInstanceId();
                result = liveHosts.get(0).getInstanceId();

                TokenRetrieverUtils.inferTokenOwnerFromGossip(
                        liveHosts, liveHosts.get(0).getToken(), liveHosts.get(0).getDC());
                result = liveHosts.get(0).getHostIP();
            }
        };

        IDeadTokenRetriever deadTokenRetriever =
                new DeadTokenRetriever(factory, membership, config, sleeper, instanceInfo);
        IPreGeneratedTokenRetriever preGeneratedTokenRetriever =
                new PreGeneratedTokenRetriever(factory, membership, config, sleeper, instanceInfo);
        INewTokenRetriever newTokenRetriever =
                new NewTokenRetriever(
                        factory, membership, config, sleeper, tokenManager, instanceInfo);
        InstanceIdentity instanceIdentity =
                new InstanceIdentity(
                        factory,
                        membership,
                        config,
                        sleeper,
                        tokenManager,
                        deadTokenRetriever,
                        preGeneratedTokenRetriever,
                        newTokenRetriever,
                        instanceInfo);

        Assert.assertEquals(false, instanceIdentity.isReplace());
    }

    @Test
    public void grabAssignedTokenStartDbInReplaceModeWhenGossipAgreesOnPreviousTokenOwner(
            @Mocked IPriamInstanceFactory<PriamInstance> factory,
            @Mocked IConfiguration config,
            @Mocked IMembership membership,
            @Mocked Sleeper sleeper,
            @Mocked ITokenManager tokenManager,
            @Mocked InstanceInfo instanceInfo,
            @Mocked TokenRetrieverUtils retrievalUtils)
            throws Exception {
        List<PriamInstance> liveHosts = newPriamInstances();
        Collections.shuffle(liveHosts);

        PriamInstance deadInstance = liveHosts.remove(0);
        PriamInstance newInstance =
                newMockPriamInstance(
                        APP,
                        deadInstance.getDC(),
                        deadInstance.getRac(),
                        deadInstance.getId(),
                        String.format("new-fakeInstance-%d", deadInstance.getId()),
                        String.format("127.1.1.%d", deadInstance.getId() + 100),
                        String.format("new-fakeHost-%d", deadInstance.getId()),
                        deadInstance.getToken());

        // the case we are trying to test is when Priam restarted after it acquired the
        // token. new instance is already registered with token database.
        liveHosts.add(newInstance);

        new Expectations() {
            {
                config.getAppName();
                result = APP;

                factory.getAllIds(DEAD_APP);
                result = Collections.singletonList(deadInstance);
                factory.getAllIds(APP);
                result = liveHosts;

                instanceInfo.getInstanceId();
                result = newInstance.getInstanceId();

                TokenRetrieverUtils.inferTokenOwnerFromGossip(
                        liveHosts, newInstance.getToken(), newInstance.getDC());
                result = deadInstance.getHostIP();
            }
        };

        IDeadTokenRetriever deadTokenRetriever =
                new DeadTokenRetriever(factory, membership, config, sleeper, instanceInfo);
        IPreGeneratedTokenRetriever preGeneratedTokenRetriever =
                new PreGeneratedTokenRetriever(factory, membership, config, sleeper, instanceInfo);
        INewTokenRetriever newTokenRetriever =
                new NewTokenRetriever(
                        factory, membership, config, sleeper, tokenManager, instanceInfo);
        InstanceIdentity instanceIdentity =
                new InstanceIdentity(
                        factory,
                        membership,
                        config,
                        sleeper,
                        tokenManager,
                        deadTokenRetriever,
                        preGeneratedTokenRetriever,
                        newTokenRetriever,
                        instanceInfo);

        Assert.assertEquals(deadInstance.getHostIP(), instanceIdentity.getReplacedIp());
        Assert.assertEquals(true, instanceIdentity.isReplace());
    }

    @Test
    public void grabAssignedTokenStartDbInBootstrapModeWhenGossipDisagreesOnPreviousTokenOwner(
            @Mocked IPriamInstanceFactory<PriamInstance> factory,
            @Mocked IConfiguration config,
            @Mocked IMembership membership,
            @Mocked Sleeper sleeper,
            @Mocked ITokenManager tokenManager,
            @Mocked InstanceInfo instanceInfo,
            @Mocked TokenRetrieverUtils retrievalUtils)
            throws Exception {
        List<PriamInstance> liveHosts = newPriamInstances();
        Collections.shuffle(liveHosts);

        new Expectations() {
            {
                config.getAppName();
                result = APP;

                factory.getAllIds(DEAD_APP);
                result = Collections.emptyList();
                factory.getAllIds(APP);
                result = liveHosts;

                instanceInfo.getInstanceId();
                result = liveHosts.get(0).getInstanceId();

                TokenRetrieverUtils.inferTokenOwnerFromGossip(
                        liveHosts, liveHosts.get(0).getToken(), liveHosts.get(0).getDC());
                result = null;
            }
        };

        IDeadTokenRetriever deadTokenRetriever =
                new DeadTokenRetriever(factory, membership, config, sleeper, instanceInfo);
        IPreGeneratedTokenRetriever preGeneratedTokenRetriever =
                new PreGeneratedTokenRetriever(factory, membership, config, sleeper, instanceInfo);
        INewTokenRetriever newTokenRetriever =
                new NewTokenRetriever(
                        factory, membership, config, sleeper, tokenManager, instanceInfo);
        InstanceIdentity instanceIdentity =
                new InstanceIdentity(
                        factory,
                        membership,
                        config,
                        sleeper,
                        tokenManager,
                        deadTokenRetriever,
                        preGeneratedTokenRetriever,
                        newTokenRetriever,
                        instanceInfo);

        Assert.assertTrue(StringUtils.isEmpty(instanceIdentity.getReplacedIp()));
        Assert.assertEquals(false, instanceIdentity.isReplace());
    }

    private List<PriamInstance> newPriamInstances() {
        List<PriamInstance> instances = new ArrayList<>();

        instances.addAll(newPriamInstances("eu-west", "1a", 0, "127.3.1.%d"));
        instances.addAll(newPriamInstances("eu-west", "1b", 3, "127.3.2.%d"));
        instances.addAll(newPriamInstances("eu-west", "1c", 6, "127.3.3.%d"));

        instances.addAll(newPriamInstances("us-east", "1c", 1, "127.1.3.%d"));
        instances.addAll(newPriamInstances("us-east", "1a", 4, "127.1.1.%d"));
        instances.addAll(newPriamInstances("us-east", "1b", 7, "127.1.2.%d"));

        instances.addAll(newPriamInstances("us-west-2", "2a", 2, "127.2.1.%d"));
        instances.addAll(newPriamInstances("us-west-2", "2b", 5, "127.2.2.%d"));
        instances.addAll(newPriamInstances("us-west-2", "2c", 8, "127.2.3.%d"));

        return instances;
    }

    private List<PriamInstance> newPriamInstances(
            String dc, String rack, int seqNo, String ipRanges) {
        return IntStream.range(0, 3)
                .map(e -> seqNo + (e * 9))
                .<PriamInstance>mapToObj(
                        e ->
                                newMockPriamInstance(
                                        APP,
                                        dc,
                                        rack,
                                        e,
                                        String.format("fakeInstance-%d", e),
                                        String.format(ipRanges, e),
                                        String.format("fakeHost-%d", e),
                                        Integer.toString(e)))
                .collect(Collectors.toList());
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
