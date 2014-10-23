package com.netflix.priam.cryptography;

import java.io.File;

/*
 * A generic, unimplemented class to store / fetch keys needed for various crypotography algorithm.  For example,
 * you can provide an implementation where your key(s) are encrypted/decrypted using AES encryption algorithm or your encryption strategy is to merely
 * return the plaintext. 
 *
 * It is expected that you provide your own implementation, this class is here eliminate compilation / run-time errors.  
 */
public class GenericKeyCryptography  implements IKeyCryptography {

	@Override
	/*
	 * @param plaintext - key in plaintext to encrypt to ciphertext.
	 * @return encrypted text
	 */		
	public String enccrypt(String plaintext) {
		throw new UnsupportedOperationException("Dont' use this generic class, you need to implemented this behavior.");
	}

	@Override
	public String decrypt(String ciphertext) {
		throw new UnsupportedOperationException("Dont' use this generic class, you need to implemented this behavior.");
	}

	@Override
	public String enccrypt(File plaintext) {
		throw new UnsupportedOperationException("Dont' use this generic class, you need to implemented this behavior.");
	}

	@Override
	public byte[] decrypt(File ciphertext) {
		throw new UnsupportedOperationException("Dont' use this generic class, you need to implemented this behavior.");
	}	
	
}
