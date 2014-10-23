package com.netflix.priam.cryptography;

import java.io.InputStream;
import java.util.Iterator;

public interface IFileCryptography {
	
    /*
     * @param in - a handle to the encrypted, compressed data stream
     * @param pass - pass phrase used to extract the PGP private key from the encrypted content.
     * @param objectName - name of the object we are decrypting, currently use for debugging purposes only.
     * @return a handle to the decrypted, uncompress data stream.
     */    
	public InputStream decryptStream(InputStream in, char[] passwd, String objectName) throws Exception;
	
	/*
	 * @aparam is - a handle to the plaintext data stream
	 * @return - an iterate of the ciphertext stream
	 */
	public Iterator<byte[]> encryptStream(InputStream is, String fileName) throws Exception;
    
    
}