package com.bazaarvoice.priam.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/** HTTP client implements remote access to a System of Record server. */
public class PriamCassAdminClient implements PriamCassAdmin {

    /** Must match the service name in the EmoService class. */
    /*package*/ static final String BASE_SERVICE_NAME = "emodb-sor-1";

    /** Must match the @Path annotation on the DataStoreResource class. */
    public static final String SERVICE_PATH = "/v1";

    private static final Duration UPDATE_ALL_REQUEST_DURATION = Duration.standardSeconds(1);

    private final Client _client;
    private final UriBuilder _priamCassAdmin;

    public PriamCassAdminClient(URI endPoint, Client jerseyClient) {
        _client = checkNotNull(jerseyClient, "jerseyClient");
        _priamCassAdmin = UriBuilder.fromUri(endPoint);
    }

    @Override
    public List<Map<String, Object>> getHintsForTheRing(){
        try {
//            URI uri = _priamCassAdmin.clone()
//                    .segment("_table")
//                    .queryParam("from", optional(fromTableExclusive))
//                    .queryParam("limit", limit)
//                    .build();
            URI uri = _priamCassAdmin.clone()
                    .segment("cassadmin","hints","ring")
                    .build();
            String hints = _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .get(String.class);

            return JsonHelper.deserialized(hints, new TypeReference<List<Map<String, Object>>>() {});
        } catch (UniformInterfaceException e) {
            //throw convertException(e);
            throw e;
        }
    }

//    @SuppressWarnings ("ThrowableResultOfMethodCallIgnored")
//    private RuntimeException convertException(UniformInterfaceException e) {
//        ClientResponse response = e.getResponse();
//        String exceptionType = response.getHeaders().getFirst("X-BV-Exception");
//
//        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode() &&
//                IllegalArgumentException.class.getName().equals(exceptionType)) {
//            return new IllegalArgumentException(response.getEntity(String.class), e);
//
//        } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode() &&
//                TableExistsException.class.getName().equals(exceptionType)) {
//            if (response.hasEntity()) {
//                return (RuntimeException) response.getEntity(TableExistsException.class).initCause(e);
//            } else {
//                return (RuntimeException) new TableExistsException().initCause(e);
//            }
//
//        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
//                UnknownTableException.class.getName().equals(exceptionType)) {
//            if (response.hasEntity()) {
//                return (RuntimeException) response.getEntity(UnknownTableException.class).initCause(e);
//            } else {
//                return (RuntimeException) new UnknownTableException().initCause(e);
//            }
//
//        } else if (response.getStatus() == Response.Status.MOVED_PERMANENTLY.getStatusCode() &&
//                UnsupportedOperationException.class.getName().equals(exceptionType)) {
//            return new UnsupportedOperationException("Permanent redirect: " + response.getLocation(), e);
//        }
//        return e;
//    }


}
