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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPOnePassSignatureList;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.util.io.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.cli.Application;
import com.netflix.priam.cryptography.IFileCryptography;

public class PgpCryptography implements IFileCryptography {
	private static final Logger logger = LoggerFactory.getLogger(PgpCryptography.class);
	
	private IConfiguration config;

    static 
    {
        Security.addProvider(new BouncyCastleProvider()); //tell the JVM the security provider is PGP
    }	
	
	@Inject
	public PgpCryptography(IConfiguration config) {
		
		this.config = config;

		
	}
	
	private PGPSecretKeyRingCollection getPgpSecurityCollection() {

		InputStream keyIn;
		try {
			keyIn = new BufferedInputStream(new FileInputStream(config.getPrivateKeyLocation()));
		} catch (FileNotFoundException e) {
			throw new IllegalStateException("PGP private key file not found.  file: " + config.getPrivateKeyLocation());
		}
		
		try {
			
			return new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(keyIn));
			
		} catch (Exception e) {
			logger.error("Exception in reading PGP security collection ring.  Msg: " + e.getLocalizedMessage());
			throw new IllegalStateException("Exception in reading PGP security collection ring", e);
		}		
		
	}
	
	private PGPPublicKey getPubKey() {
		InputStream pubKeyIS;
		try {
			pubKeyIS = new BufferedInputStream(new FileInputStream(config.getPgpPublicKeyLoc()));
			
		} catch (FileNotFoundException e) {
			logger.error("Exception in reading PGP security collection ring.  Msg: " + e.getLocalizedMessage());
			throw new RuntimeException("Exception in reading PGP public key", e);
		}
		
		try {
			 
			return PgpUtil.readPublicKey(pubKeyIS);
			 
		} catch (Exception e) {
			throw new RuntimeException("Exception in reading & deriving the PGP public key.", e);
		}		
	}

    /*
     * @param in - a handle to the encrypted, compressed data stream
     * @param pass - pass phrase used to extract the PGP private key from the encrypted content.
     * @param objectName - name of the object used only for debugging purposes
     * @return a handle to the decrypted, uncompress data stream.
     */	
	@Override
	public InputStream decryptStream(InputStream in, char[] passwd, String objectName) throws Exception {
		
		logger.info("Start to decrypt object: " + objectName);
		
		in = PGPUtil.getDecoderStream(in);
		
		PGPObjectFactory inPgpReader = new PGPObjectFactory(in); //general class for reading a stream of data.
		Object o = inPgpReader.nextObject();
		
		PGPEncryptedDataList encryptedDataList;
        // the first object might be a PGP marker packet.
        if (o instanceof PGPEncryptedDataList)
        	encryptedDataList = (PGPEncryptedDataList) o;
        else
        	encryptedDataList = (PGPEncryptedDataList) inPgpReader.nextObject(); //first object was a marker, the real data is the next one.
        
        Iterator encryptedDataIterator = encryptedDataList.getEncryptedDataObjects();  //get the iterator so we can iterate through all the encrypted data.
        
        PGPPrivateKey privateKey = null; //to be use for decryption
        PGPPublicKeyEncryptedData encryptedDataStreamHandle = null; //a handle to the encrypted data stream
        while (privateKey == null && encryptedDataIterator.hasNext())
        {
        	encryptedDataStreamHandle = (PGPPublicKeyEncryptedData) encryptedDataIterator.next(); //a handle to the encrypted data stream
            
        	try{
            	privateKey = findSecretKey(getPgpSecurityCollection(), encryptedDataStreamHandle.getKeyID(), passwd);        		
        	} catch (Exception ex) {
        		throw new IllegalStateException("decryption exception:  object: " + objectName + ", Exception when fetching private key using key: " + encryptedDataStreamHandle.getKeyID(), ex);
        	}

        }
        if (privateKey == null)
            throw new IllegalStateException("decryption exception:  object: " + objectName + ", Private key for message not found.");
        
        //finally, lets decrypt the object
        InputStream decryptInputStream = encryptedDataStreamHandle.getDataStream(privateKey, "BC");
        PGPObjectFactory decryptedDataReader = new PGPObjectFactory(decryptInputStream);
        
        //the decrypted data object is compressed, lets decompress it.
        PGPCompressedData comporessedDataReader = (PGPCompressedData) decryptedDataReader.nextObject(); //get a handle to the decrypted, compress data stream
        InputStream compressedStream = new BufferedInputStream(comporessedDataReader.getDataStream());
        PGPObjectFactory compressedStreamReader = new PGPObjectFactory(compressedStream);
        Object data = compressedStreamReader.nextObject();
        if (data instanceof PGPLiteralData)
        {
            PGPLiteralData dataPgpReader = (PGPLiteralData) data;
            return dataPgpReader.getInputStream(); //a handle to the decrypted, uncompress data stream
            
        }  else if (data instanceof PGPOnePassSignatureList) {
        	throw new PGPException("decryption exception:  object: " + objectName + ", encrypted data contains a signed message - not literal data.");
        } else {
        	throw new PGPException("decryption exception:  object: " + objectName + ", data is not a simple encrypted file - type unknown.");
        }
                    
        
        
	}
	
	/*
	 * Extract the PGP private key from the encrypted content.  Since the PGP key file contains N number of keys, this method will fetch the 
	 * private key by "keyID".
	 * 
	 * @param securityCollection - handle to the PGP key file.
	 * @param keyID - fetch private key for this value.
	 * @param pass - pass phrase used to extract the PGP private key from the encrypted content.
	 * @return PGP private key, null if not found.
	 */
	private static PGPPrivateKey findSecretKey(PGPSecretKeyRingCollection securityCollection, long keyID, char[] pass) throws PGPException, NoSuchProviderException {
		
		PGPSecretKey privateKey = securityCollection.getSecretKey(keyID);
        if (privateKey == null)
        {
            return null;
        }

        return privateKey.extractPrivateKey(pass, "BC");		
		
	}
	
	@Override
	public Iterator<byte[]> encryptStream(InputStream is, String fileName) {
		return new ChunkEncryptorStream(is, fileName, getPubKey());
	}
	
	public class ChunkEncryptorStream implements Iterator<byte[]> {

		// Chunk sizes of 10 MB
	    private static final int MAX_CHUNK = 10 * 1024 * 1024;
		
		private boolean hasnext = true;
		private InputStream is;
		private InputStream encryptedSrc;
		private ByteArrayOutputStream bos;
		private BufferedOutputStream pgout;

		public ChunkEncryptorStream(InputStream is, String fileName, PGPPublicKey pubKey) {
			this.is = is;
			
			this.bos = new ByteArrayOutputStream();
			this.pgout = new BufferedOutputStream(this.bos);
			this.encryptedSrc = new EncryptedInputStream(this.is, fileName, pubKey);
		}
		
		@Override
		public boolean hasNext() {
			return this.hasnext;
			
		}

		/*
		 * Fill and return a buffer of the data within encrypted stream.
		 * 
		 * @return a buffer of ciphertext
		 */
		@Override
		public byte[] next() {
			try {
				
				byte buffer[] = new byte[2048];
				int count;
                while ((count = encryptedSrc.read(buffer, 0, buffer.length)) != -1)
                {
                    pgout.write(buffer, 0, count);
                    if (bos.size() >= MAX_CHUNK)
                        return returnSafe();
                }
                return done(); //flush remaining data in buffer and close resources.
				
			} catch (Exception e) {
				throw new RuntimeException("Error encountered returning next chunk of ciphertext.  Msg: " + e.getLocalizedMessage(), e);
			}
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			
		}
		
		/*
		 * Copy the data in the buffer to the output[] and then reset the buffer to the beginning.
		 */
		private byte[] returnSafe() {
			byte[] returnData = this.bos.toByteArray();
			this.bos.reset();
			return returnData;
		}
		
		/*
		 * flush remaining data in buffer and close resources.
		 */
		private byte[] done() throws IOException {
			pgout.flush(); //flush whatever is in the buffer to the output stream
			
			this.hasnext = false; //tell clients that there is no more data
			
			byte[] returnData = this.bos.toByteArray();
			IOUtils.closeQuietly(pgout); //close the handle to the buffered output
            IOUtils.closeQuietly(bos); //close the handle to the actual output
            
            return returnData;

		}
		
	}
	
	public class EncryptedInputStream extends InputStream {
		
		private InputStream srcHandle; //handle to the source stream
		private ByteArrayOutputStream bos = null; //Handle to encrypted stream
		private int bosOff = 0; //current position within encrypted stream
		private OutputStream pgpBosWrapper; //wrapper around the buffer which will contain the encrypted data.
		private OutputStream encryptedOsWrapper; //handle to the encrypted data
		private PGPCompressedDataGenerator compressedDataGenerator; //a means to compress data using PGP
		private String fileName; //TODO: eliminate once debugging is completed.

		public EncryptedInputStream(InputStream is, String fileName, PGPPublicKey pubKey) {
			this.srcHandle = is;
			this.bos = new ByteArrayOutputStream();

			//creates a cipher stream which will have an integrity packet assocaited with it
			PGPEncryptedDataGenerator encryptedDataGenerator = new PGPEncryptedDataGenerator(PGPEncryptedData.CAST5, true, new SecureRandom(), "BC");
			try {
				encryptedDataGenerator.addMethod(pubKey); //Add a key encryption method to be used to encrypt the session data associated with this encrypted data
				pgpBosWrapper = encryptedDataGenerator.open(bos, new byte[1 << 15]); //wrapper around the buffer which will contain the encrypted data.				
			} catch (Exception e) {
				throw new RuntimeException("Exception when wrapping PGP around our output stream", e);
			}

			//a means to compress data using PGP
			this.compressedDataGenerator = new PGPCompressedDataGenerator(PGPCompressedData.UNCOMPRESSED);

			/*
			 * Open a literal data packet, returning a stream to store the data inside the packet as an indefinitite stream.
			 * A "literal data packet" in PGP world is the body of a message; data that is not to be further interpreted.
			 * 
			 * The stream is written out as a series of partial packets with a chunk size determine by the size of the passed in buffer.
			 * @param outputstream - the stream we want the packet in
			 * @param format - the format we are using.
			 * @param filename
			 * @param the time of last modification we want stored.
			 * @param the buffer to use for collecting data to put into chunks.
			 */
			try {
				PGPLiteralDataGenerator literalDataGenerator = new PGPLiteralDataGenerator();
				this.encryptedOsWrapper = literalDataGenerator.open(compressedDataGenerator.open(pgpBosWrapper), PGPLiteralData.BINARY, fileName, new Date(), new byte[1 << 15]);				
			} catch (Exception e) {
				throw new RuntimeException("Exception when creating the PGP encrypted wrapper around the output stream.", e);
			}
			
			this.fileName = fileName;  //TODO: eliminate once debugging is completed.
			

		}
		
		/*
		 * Read a chunk from input stream and perform encryption.
		 * 
		 * 
		 * @param buffer for this behavior to store the encrypted behavior
		 * @param starting position within buffer to append
		 * @param max number of bytes to store in buffer
		 */
		@Override
		public synchronized int read(byte b[], int off, int len) throws IOException {
			if (this.bosOff < this.bos.size()) {
				//if here, you still have data in the encrypted stream, lets give it to the client
				return copyToBuff(b, off, len);
			}
			
			//If here, it's time to read the next chunk from the input and do the encryption.
			this.bos.reset();
			this.bosOff = 0;
			
			//== read up to "len" or end of file from input stream and encrypt it.
			
			byte[] buff = new byte[1 << 16];
			int bytesRead = 0; //num of bytes read from the source input stream
			
			while (this.bos.size() < len && (bytesRead = this.srcHandle.read(buff, 0, len)) > 0 ) { //lets process each chunk from input until we fill our output stream or we reach end of input
				
				/* TODO: msg was only for debug purposes
				 * 
				logger.info("Reading input file: " + this.fileName + ", number of bytes read from input stream: " + bytesRead 
						+ ", size of buffer: " 
						+ buff.length
						);
						
				*/
				
				this.encryptedOsWrapper.write(buff, 0, bytesRead);
			}
			
			if (bytesRead < 0) { //we have read everything from the source input, lets perform cleanup on any resources.
				this.encryptedOsWrapper.close();
				this.compressedDataGenerator.close();
				this.pgpBosWrapper.close();
			}
			
			if (bytesRead < 0 && this.bos.size() == 0) {
				//if here, read all the bytes from the input and there is nothing in the encrypted stream.  
				return bytesRead;
			}
			
			/*
			 * If here, one of the following occurred:
			 * 1. you read data from the input and encrypted it.
			 * 2. there was no more data in the input but you still had some data in the encrypted stream. 
			 * 
			 */
			return copyToBuff(b, off, len);
		}
		
		/*
		 * 
		 * Copy the bytes from the encrypted stream to an output buffer
		 * 
		 * @param output buffer
		 * @param starting point within output buffer
		 * @param max size of output buffer
		 * @return number of bytes copied from the encrypted stream to the output buffer
		 */
		private int copyToBuff(byte[] buff, int off, int len) {
			/*
			 * num of bytes to copy within encrypted stream = (current size of bytes within encrypted stream - current position within encrypted stream)  < size of output buffer, 
			 * then copy what is in the encrypted stream; otherwise, copy up to the max size of the output buffer. 
			 */
			int wlen = (this.bos.size() - this.bosOff) < len ? (this.bos.size() - this.bosOff) : len;
			System.arraycopy(this.bos.toByteArray(), this.bosOff, buff, off, wlen); //copy data within encrypted stream to the output buffer
			
			this.bosOff = this.bosOff + wlen; //now update the current position within the encrypted stream
			return wlen;
		}
		
		@Override
		public void close() throws IOException {
			this.encryptedOsWrapper.close();
			this.compressedDataGenerator.close();
			this.pgpBosWrapper.close();
		}

		@Override
		public int read() throws IOException {
			throw new UnsupportedOperationException("Not supported, invoke read(byte[] bytes, int off, int len) instead.");
		}
		
	}
}