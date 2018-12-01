/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.google;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.AbstractFileSystem;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.cred.ICredentialGeneric;
import com.netflix.priam.cred.ICredentialGeneric.KEY;
import com.netflix.priam.merics.BackupMetrics;
import com.netflix.priam.notification.BackupNotificationMgr;
import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleEncryptedFileSystem extends AbstractFileSystem {

    private static final Logger logger = LoggerFactory.getLogger(GoogleEncryptedFileSystem.class);

    private static final String APPLICATION_NAME = "gdl";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    private HttpTransport httpTransport;
    // represents our "service account" credentials we will use to access GCS
    private Credential credential;
    private Storage gcsStorageHandle;
    private Storage.Objects objectsResoruceHandle = null;
    private String srcBucketName;
    private final IConfiguration config;

    private final ICredentialGeneric gcsCredential;
    private final BackupMetrics backupMetrics;

    @Inject
    public GoogleEncryptedFileSystem(
            Provider<AbstractBackupPath> pathProvider,
            final IConfiguration config,
            @Named("gcscredential") ICredentialGeneric credential,
            BackupMetrics backupMetrics,
            BackupNotificationMgr backupNotificationManager) {
        super(config, backupMetrics, backupNotificationManager, pathProvider);
        this.backupMetrics = backupMetrics;
        this.config = config;
        this.gcsCredential = credential;

        try {
            this.httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to create a handle to the Google Http tranport", e);
        }

        this.srcBucketName = getBucket();
    }

    private Storage.Objects constructObjectResourceHandle() {
        if (this.objectsResoruceHandle != null) {
            return this.objectsResoruceHandle;
        }

        constructGcsStorageHandle();
        this.objectsResoruceHandle = this.gcsStorageHandle.objects();
        return this.objectsResoruceHandle;
    }

    /*
     * Get a handle to the GCS api to manage our data within their storage.  Code derive from
     * https://code.google.com/p/google-api-java-client/source/browse/storage-cmdline-sample/src/main/java/com/google/api/services/samples/storage/cmdline/StorageSample.java?repo=samples
     *
     * Note: GCS storage will use our credential to do auto-refresh of expired tokens
     */
    private Storage constructGcsStorageHandle() {
        if (this.gcsStorageHandle != null) {
            return this.gcsStorageHandle;
        }

        try {
            constructGcsCredential();
        } catch (Exception e) {
            throw new IllegalStateException("Exception during GCS authorization", e);
        }

        this.gcsStorageHandle =
                new Storage.Builder(this.httpTransport, JSON_FACTORY, this.credential)
                        .setApplicationName(APPLICATION_NAME)
                        .build();
        return this.gcsStorageHandle;
    }

    /**
     * Authorizes the installed application to access user's protected data, code from
     * https://developers.google.com/maps-engine/documentation/oauth/serviceaccount and
     * http://javadoc.google-api-java-client.googlecode.com/hg/1.8.0-beta/com/google/api/client/googleapis/auth/oauth2/GoogleCredential.html
     */
    private Credential constructGcsCredential() throws Exception {

        if (this.credential != null) {
            return this.credential;
        }

        synchronized (this) {
            if (this.credential == null) {

                String service_acct_email =
                        new String(this.gcsCredential.getValue(KEY.GCS_SERVICE_ID));

                if (this.config.getGcsServiceAccountPrivateKeyLoc() == null
                        || this.config.getGcsServiceAccountPrivateKeyLoc().isEmpty()) {
                    throw new NullPointerException(
                            "Fast property for the the GCS private key file is null/empty.");
                }

                // Take the encrypted private key, decrypted into an in-transit file which is passed
                // to GCS
                File gcsPrivateKeyHandle =
                        new File(this.config.getGcsServiceAccountPrivateKeyLoc() + ".output");

                ByteArrayOutputStream byteos = new ByteArrayOutputStream();

                byte[] gcsPrivateKeyPlainText =
                        this.gcsCredential.getValue(KEY.GCS_PRIVATE_KEY_LOC);
                try (BufferedOutputStream bos =
                        new BufferedOutputStream(new FileOutputStream(gcsPrivateKeyHandle))) {
                    byteos.write(gcsPrivateKeyPlainText);
                    byteos.writeTo(bos);
                } catch (IOException e) {
                    throw new IOException(
                            "Exception when writing decrypted gcs private key value to disk.", e);
                }

                Collection<String> scopes = new ArrayList<>(1);
                scopes.add(StorageScopes.DEVSTORAGE_READ_ONLY);
                // Cryptex decrypted service account key derive from the GCS console
                this.credential =
                        new GoogleCredential.Builder()
                                .setTransport(this.httpTransport)
                                .setJsonFactory(JSON_FACTORY)
                                .setServiceAccountId(service_acct_email)
                                .setServiceAccountScopes(scopes)
                                .setServiceAccountPrivateKeyFromP12File(gcsPrivateKeyHandle)
                                .build();
            }
        }

        return this.credential;
    }

    @Override
    protected void downloadFileImpl(Path remotePath, Path localPath) throws BackupRestoreException {
        String objectName = parseObjectname(getPrefix().toString());
        com.google.api.services.storage.Storage.Objects.Get get;

        try {
            get = constructObjectResourceHandle().get(this.srcBucketName, remotePath.toString());
        } catch (IOException e) {
            throw new BackupRestoreException(
                    "IO error retrieving metadata for: "
                            + objectName
                            + " from bucket: "
                            + this.srcBucketName,
                    e);
        }

        // If you're not using GCS' AppEngine, download the whole thing (instead of chunks) in one
        // request, if possible.
        get.getMediaHttpDownloader().setDirectDownloadEnabled(true);
        try (OutputStream os = new FileOutputStream(localPath.toFile());
                InputStream is = get.executeMediaAsInputStream()) {
            IOUtils.copyLarge(is, os);
        } catch (IOException e) {
            throw new BackupRestoreException(
                    "IO error during streaming of object: "
                            + objectName
                            + " from bucket: "
                            + this.srcBucketName,
                    e);
        } catch (Exception ex) {
            throw new BackupRestoreException(
                    "Exception encountered when copying bytes from input to output", ex);
        }

        backupMetrics.recordDownloadRate(get.getLastResponseHeaders().getContentLength());
    }

    @Override
    public Iterator<String> list(String prefix, String delimiter) {
        return new GoogleFileIterator(constructGcsStorageHandle(), prefix, null);
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub
    }

    @Override
    protected long uploadFileImpl(Path localPath, Path remotePath) throws BackupRestoreException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFileSize(Path remotePath) throws BackupRestoreException {
        return 0;
    }

    /*
     * @param pathPrefix
     * @return objectName
     */
    static String parseObjectname(String pathPrefix) {
        int offset = pathPrefix.lastIndexOf(0x2f);
        return pathPrefix.substring(offset + 1);
    }
}
