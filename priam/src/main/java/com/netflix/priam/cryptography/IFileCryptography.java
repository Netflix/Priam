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
package com.netflix.priam.cryptography;

import java.io.InputStream;
import java.util.Iterator;

public interface IFileCryptography {

    enum CryptographyAlgorithm {
        PLAINTEXT,
        PGP
    }

    /**
     * @param in - a handle to the encrypted, compressed data stream
     * @param passwd - pass phrase used to extract the PGP private key from the encrypted content.
     * @param objectName - name of the object we are decrypting, currently use for debugging
     *     purposes only.
     * @return a handle to the decrypted, uncompress data stream.
     */
    InputStream decryptStream(InputStream in, char[] passwd, String objectName) throws Exception;

    /**
     * @param is - a handle to the plaintext data stream
     * @return - an iterate of the ciphertext stream
     */
    Iterator<byte[]> encryptStream(InputStream is, String fileName) throws Exception;
}
