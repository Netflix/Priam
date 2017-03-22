/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.cryptography.pgp;

import java.io.File;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.ICredentialGeneric;

/*
 * A generic implementation of fetch keys as plaintext.  The key values are used within PGP cryptography algorithm.  Users may
 * want to provide an implementation where your key(s)' value is decrypted using AES encryption algorithm.  
 */
public class PgpCredential  implements ICredentialGeneric {

	private IConfiguration config;

	@Inject
	public PgpCredential(IConfiguration config) {
		this.config = config;
	}
	
	@Override
	public AWSCredentialsProvider getAwsCredentialProvider() {
		return null;
	}

	@Override
	public byte[] getValue(KEY key) {
		if (key == null) {
			throw new NullPointerException("Credential key cannot be null.");
		}
		
		if (key.equals(KEY.PGP_PASSWORD)) {
			return this.config.getPgpPasswordPhrase().getBytes();			
		} else if (key.equals(KEY.PGP_PUBLIC_KEY_LOC)) {
			return this.config.getPgpPublicKeyLoc().getBytes();
		} else {
			throw new IllegalArgumentException("Key value not supported.");
		}

	}


}
