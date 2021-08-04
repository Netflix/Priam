package com.netflix.priam.identity.token;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.truth.Truth;
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
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class AssignedTokenRetrieverTest {
    public static final String APP = "testapp";
    public static final String DEAD_APP = "testapp-dead";

    @Test
    public void grabAssignedTokenStartDbInBootstrapModeWhenGossipAgreesCurrentInstanceIsTokenOwner(
            @Mocked IPriamInstanceFactory factory,
            @Mocked IConfiguration config,
            @Mocked IMembership membership,
            @Mocked Sleeper sleeper,
            @Mocked ITokenManager tokenManager,
            @Mocked InstanceInfo instanceInfo,
            @Mocked TokenRetrieverUtils retrievalUtils)
            throws Exception {
        List<PriamInstance> liveHosts = newPriamInstances();
        Collections.shuffle(liveHosts);

        TokenRetrieverUtils.InferredTokenOwnership inferredTokenOwnership =
                new TokenRetrieverUtils.InferredTokenOwnership();
        inferredTokenOwnership.setTokenInformationStatus(
                TokenRetrieverUtils.InferredTokenOwnership.TokenInformationStatus.GOOD);
        inferredTokenOwnership.setTokenInformation(
                new TokenRetrieverUtils.TokenInformation(liveHosts.get(0).getHostIP(), false));

        new Expectations() {
            {
                config.getAppName();
                result = APP;

                factory.getAllIds(DEAD_APP);
                result = ImmutableSet.of();

                factory.getAllIds(APP);
                result = ImmutableSet.copyOf(liveHosts);

                instanceInfo.getInstanceId();
                result = liveHosts.get(0).getInstanceId();

                instanceInfo.getHostIP();
                result = liveHosts.get(0).getHostIP();

                TokenRetrieverUtils.inferTokenOwnerFromGossip(
                        ImmutableSet.copyOf(liveHosts),
                        liveHosts.get(0).getToken(),
                        liveHosts.get(0).getDC());
                result = inferredTokenOwnership;
            }
        };

        ITokenRetriever tokenRetriever =
                new TokenRetriever(
                        factory, membership, config, instanceInfo, sleeper, tokenManager);
        InstanceIdentity instanceIdentity =
                new InstanceIdentity(factory, membership, config, instanceInfo, tokenRetriever);
        Truth.assertThat(instanceIdentity.isReplace()).isFalse();
    }

    @Test
    public void grabAssignedTokenStartDbInReplaceModeWhenGossipAgreesPreviousTokenOwnerIsNotLive(
            @Mocked IPriamInstanceFactory factory,
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
        TokenRetrieverUtils.InferredTokenOwnership inferredTokenOwnership =
                new TokenRetrieverUtils.InferredTokenOwnership();
        inferredTokenOwnership.setTokenInformationStatus(
                TokenRetrieverUtils.InferredTokenOwnership.TokenInformationStatus.GOOD);
        inferredTokenOwnership.setTokenInformation(
                new TokenRetrieverUtils.TokenInformation(deadInstance.getHostIP(), false));

        new Expectations() {
            {
                config.getAppName();
                result = APP;

                factory.getAllIds(DEAD_APP);
                result = ImmutableSet.of(deadInstance);
                factory.getAllIds(APP);
                result = ImmutableSet.copyOf(liveHosts);

                instanceInfo.getInstanceId();
                result = newInstance.getInstanceId();

                TokenRetrieverUtils.inferTokenOwnerFromGossip(
                        ImmutableSet.copyOf(liveHosts),
                        newInstance.getToken(),
                        newInstance.getDC());
                result = inferredTokenOwnership;
            }
        };

        ITokenRetriever tokenRetriever =
                new TokenRetriever(
                        factory, membership, config, instanceInfo, sleeper, tokenManager);
        InstanceIdentity instanceIdentity =
                new InstanceIdentity(factory, membership, config, instanceInfo, tokenRetriever);
        Truth.assertThat(instanceIdentity.getReplacedIp()).isEqualTo(deadInstance.getHostIP());
        Truth.assertThat(instanceIdentity.isReplace()).isTrue();
    }

    @Test
    public void grabAssignedTokenThrowWhenGossipAgreesPreviousTokenOwnerIsLive(
            @Mocked IPriamInstanceFactory factory,
            @Mocked IConfiguration config,
            @Mocked IMembership membership,
            @Mocked Sleeper sleeper,
            @Mocked ITokenManager tokenManager,
            @Mocked InstanceInfo instanceInfo,
            @Mocked TokenRetrieverUtils retrievalUtils) {
        List<PriamInstance> liveHosts = newPriamInstances();
        Collections.shuffle(liveHosts);

        PriamInstance deadInstance = liveHosts.remove(0);
        PriamInstance newInstance =
                newMockPriamInstance(
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
        TokenRetrieverUtils.InferredTokenOwnership inferredTokenOwnership =
                new TokenRetrieverUtils.InferredTokenOwnership();
        inferredTokenOwnership.setTokenInformationStatus(
                TokenRetrieverUtils.InferredTokenOwnership.TokenInformationStatus.GOOD);
        inferredTokenOwnership.setTokenInformation(
                new TokenRetrieverUtils.TokenInformation(deadInstance.getHostIP(), true));

        new Expectations() {
            {
                config.getAppName();
                result = APP;

                factory.getAllIds(DEAD_APP);
                result = ImmutableSet.of(deadInstance);
                factory.getAllIds(APP);
                result = ImmutableSet.copyOf(liveHosts);

                instanceInfo.getInstanceId();
                result = newInstance.getInstanceId();

                TokenRetrieverUtils.inferTokenOwnerFromGossip(
                        ImmutableSet.copyOf(liveHosts),
                        newInstance.getToken(),
                        newInstance.getDC());
                result = inferredTokenOwnership;
            }
        };

        ITokenRetriever tokenRetriever =
                new TokenRetriever(
                        factory, membership, config, instanceInfo, sleeper, tokenManager);
        Assertions.assertThrows(
                TokenRetrieverUtils.GossipParseException.class,
                () ->
                        new InstanceIdentity(
                                factory, membership, config, instanceInfo, tokenRetriever));
    }

    @Test
    public void grabAssignedTokenThrowToBuyTimeWhenGossipDisagreesOnPreviousTokenOwner(
            @Mocked IPriamInstanceFactory factory,
            @Mocked IConfiguration config,
            @Mocked IMembership membership,
            @Mocked Sleeper sleeper,
            @Mocked ITokenManager tokenManager,
            @Mocked InstanceInfo instanceInfo,
            @Mocked TokenRetrieverUtils retrievalUtils) {
        List<PriamInstance> liveHosts = newPriamInstances();
        Collections.shuffle(liveHosts);

        TokenRetrieverUtils.InferredTokenOwnership inferredTokenOwnership =
                new TokenRetrieverUtils.InferredTokenOwnership();
        inferredTokenOwnership.setTokenInformationStatus(
                TokenRetrieverUtils.InferredTokenOwnership.TokenInformationStatus.MISMATCH);
        inferredTokenOwnership.setTokenInformation(
                new TokenRetrieverUtils.TokenInformation(liveHosts.get(0).getHostIP(), false));

        new Expectations() {
            {
                config.getAppName();
                result = APP;

                factory.getAllIds(DEAD_APP);
                result = ImmutableSet.of();
                factory.getAllIds(APP);
                result = ImmutableSet.copyOf(liveHosts);

                instanceInfo.getInstanceId();
                result = liveHosts.get(0).getInstanceId();

                TokenRetrieverUtils.inferTokenOwnerFromGossip(
                        ImmutableSet.copyOf(liveHosts),
                        liveHosts.get(0).getToken(),
                        liveHosts.get(0).getDC());
                result = inferredTokenOwnership;
            }
        };

        ITokenRetriever tokenRetriever =
                new TokenRetriever(
                        factory, membership, config, instanceInfo, sleeper, tokenManager);
        Assertions.assertThrows(
                TokenRetrieverUtils.GossipParseException.class,
                () ->
                        new InstanceIdentity(
                                factory, membership, config, instanceInfo, tokenRetriever));
    }

    @Test
    public void grabAssignedTokenStartDbInBootstrapModeWhenGossipDisagreesOnPreviousTokenOwner(
            @Mocked IPriamInstanceFactory factory,
            @Mocked IConfiguration config,
            @Mocked IMembership membership,
            @Mocked Sleeper sleeper,
            @Mocked ITokenManager tokenManager,
            @Mocked InstanceInfo instanceInfo,
            @Mocked TokenRetrieverUtils retrievalUtils)
            throws Exception {
        List<PriamInstance> liveHosts = newPriamInstances();
        Collections.shuffle(liveHosts);

        TokenRetrieverUtils.InferredTokenOwnership inferredTokenOwnership =
                new TokenRetrieverUtils.InferredTokenOwnership();
        inferredTokenOwnership.setTokenInformationStatus(
                TokenRetrieverUtils.InferredTokenOwnership.TokenInformationStatus.MISMATCH);
        inferredTokenOwnership.setTokenInformation(
                new TokenRetrieverUtils.TokenInformation(liveHosts.get(0).getHostIP(), false));

        new Expectations() {
            {
                config.getAppName();
                result = APP;
                config.permitDirectTokenAssignmentWithGossipMismatch();
                result = true;

                factory.getAllIds(DEAD_APP);
                result = ImmutableSet.of();
                factory.getAllIds(APP);
                result = ImmutableSet.copyOf(liveHosts);

                instanceInfo.getInstanceId();
                result = liveHosts.get(0).getInstanceId();

                TokenRetrieverUtils.inferTokenOwnerFromGossip(
                        ImmutableSet.copyOf(liveHosts),
                        liveHosts.get(0).getToken(),
                        liveHosts.get(0).getDC());
                result = inferredTokenOwnership;
            }
        };

        ITokenRetriever tokenRetriever =
                new TokenRetriever(
                        factory, membership, config, instanceInfo, sleeper, tokenManager);
        InstanceIdentity instanceIdentity =
                new InstanceIdentity(factory, membership, config, instanceInfo, tokenRetriever);
        Truth.assertThat(Strings.isNullOrEmpty(instanceIdentity.getReplacedIp())).isTrue();
        Truth.assertThat(instanceIdentity.isReplace()).isFalse();
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
                .mapToObj(
                        e ->
                                newMockPriamInstance(
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
            String dc,
            String rack,
            int id,
            String instanceId,
            String hostIp,
            String hostName,
            String token) {
        PriamInstance priamInstance = new PriamInstance();
        priamInstance.setApp(APP);
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
