package com.bazaarvoice.priam.client;

import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceFactory;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.net.HttpHeaders;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;
import com.sun.jersey.client.apache4.config.ApacheHttpClient4Config;
import com.sun.jersey.client.apache4.config.DefaultApacheHttpClient4Config;
import com.yammer.dropwizard.client.HttpClientBuilder;
import com.yammer.dropwizard.client.HttpClientConfiguration;
import com.yammer.dropwizard.jersey.JacksonMessageBodyProvider;
import com.yammer.dropwizard.json.ObjectMapperFactory;
import com.yammer.dropwizard.util.Duration;
import com.yammer.dropwizard.validation.Validator;
import org.apache.http.client.HttpClient;

import java.net.URI;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class PriamClientFactory implements ServiceFactory<PriamCassAdmin> {

    public static PriamClientFactory forCluster(String clusterName) {
        HttpClientConfiguration httpClientConfiguration = new HttpClientConfiguration();
        httpClientConfiguration.setKeepAlive(Duration.seconds(1));
        return new PriamClientFactory(clusterName, createDefaultJerseyClient(httpClientConfiguration));
    }

    /**
     * Connects to Priam using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static PriamClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new PriamClientFactory(clusterName, client);
    }

    public static PriamClientFactory forClusterAndHttpConfiguration(String clusterName, HttpClientConfiguration configuration) {
        return new PriamClientFactory(clusterName, createDefaultJerseyClient(configuration));
    }

    private final String _clusterName;
    private final Client _jerseyClient;

    private PriamClientFactory(String clusterName, Client jerseyClient) {
        _clusterName = clusterName;
        _jerseyClient = jerseyClient;
    }

    private static ApacheHttpClient4 createDefaultJerseyClient(HttpClientConfiguration configuration) {
        HttpClient httpClient = new HttpClientBuilder().using(configuration).build();
        ApacheHttpClient4Handler handler = new ApacheHttpClient4Handler(httpClient, null, true);
        ApacheHttpClient4Config config = new DefaultApacheHttpClient4Config();
        config.getSingletons().add(new JacksonMessageBodyProvider(new ObjectMapperFactory().build(), new Validator()));
        return new ApacheHttpClient4(handler, config);
    }

    @Override
    public String getServiceName() {
        return _clusterName + "-cassandra";
    }

    @Override
    public void configure(ServicePoolBuilder<PriamCassAdmin> servicePoolBuilder) {
        // Defaults are ok
    }

    @Override
    public PriamCassAdmin create(ServiceEndPoint endPoint) {
        Map<?, ?> payload = JsonHelper.fromJson(endPoint.getPayload(), Map.class);
        return new PriamCassAdminClient(URI.create((String) checkNotNull(payload.get("url"))), _jerseyClient);
    }

    @Override
    public void destroy(ServiceEndPoint endPoint, PriamCassAdmin service) {
        // Nothing to do
    }

    @Override
    public boolean isRetriableException(Exception e) {
        return (e instanceof UniformInterfaceException &&
                ((UniformInterfaceException) e).getResponse().getStatus() >= 500) ||
                Iterables.any(Throwables.getCausalChain(e), Predicates.instanceOf(ClientHandlerException.class));
    }

    @Override
    public boolean isHealthy(ServiceEndPoint endPoint) {
        Map<?, ?> payload = JsonHelper.fromJson(endPoint.getPayload(), Map.class);
        URI adminUrl = URI.create((String) checkNotNull(payload.get("url")));
        return _jerseyClient.resource(adminUrl).path("/cassadmin/pingthrift")
                .header(HttpHeaders.CONNECTION, "close")
                .head().getStatus() == 200;
    }
}
