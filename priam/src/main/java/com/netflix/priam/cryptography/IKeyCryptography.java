package com.netflix.priam.cryptography;

import java.io.File;

/* 
 * A means to store and fetch key(s).  These key(s) value are to be use within your cryptography algorithms. E.g. password phrase key value  is use by PGP cryptography algorithm.
 *    
 */
public interface IKeyCryptography {

	/*
	 * @param plaintext - key in plaintext to encrypt to ciphertext.
	 * @return encrypted text in Base64 encoded format
	 */		
	public String enccrypt(String plaintext);
	/*
	 * @param plaintext to encrypt to ciphertext.
	 * @return encrypted text in Base64 encoded format
	 */	
	public String enccrypt(File plaintext);
	
	
	/*
	 * @param ciphertext - encrypted, base64 encoded ciphertext to decrypt
	 * @return plain text
	 */	
	public String decrypt(String ciphertext);
	/*
	 * @param ciphertext - encrypted, base64 encoded ciphertext to decrypt
	 * @return plain text
	 */	
	public byte[] decrypt(File ciphertext);
}
