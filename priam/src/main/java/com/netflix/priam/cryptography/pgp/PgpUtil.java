package com.netflix.priam.cryptography.pgp;

import java.security.NoSuchProviderException;
import java.util.*;
import java.io.*;

import org.bouncycastle.openpgp.*;


public class PgpUtil {

    /**
     * Search a secret key ring collection for a secret key corresponding to keyID if it
     * exists.
     * 
     * @param pgpSec a secret key ring collection.
     * @param keyID keyID we want.
     * @param pass passphrase to decrypt secret key with.
     * @return
     * @throws PGPException
     * @throws NoSuchProviderException
     */
    public static PGPPrivateKey findSecretKey(PGPSecretKeyRingCollection pgpSec, long keyID, char[] pass) throws PGPException, NoSuchProviderException {
    	
        PGPSecretKey pgpSecKey = pgpSec.getSecretKey(keyID);

        if (pgpSecKey == null)
        {
            return null;
        }

        return pgpSecKey.extractPrivateKey(pass, "BC");
    }

    public static PGPPublicKey readPublicKey(String fileName) throws IOException, PGPException
    {
        InputStream keyIn = new BufferedInputStream(new FileInputStream(fileName));
        PGPPublicKey pubKey = readPublicKey(keyIn);
        keyIn.close();
        return pubKey;
    }
    
    /**
     * A simple routine that opens a key ring file and loads the first available key
     * suitable for encryption.
     * 
     * @param input
     * @return
     * @throws IOException
     * @throws PGPException
     */
    @SuppressWarnings("rawtypes")
    public static PGPPublicKey readPublicKey(InputStream input) throws IOException, PGPException {

    	PGPPublicKeyRingCollection pgpPub = new PGPPublicKeyRingCollection(PGPUtil.getDecoderStream(input));

        //
        // we just loop through the collection till we find a key suitable for encryption, in the real
        // world you would probably want to be a bit smarter about this.
        //

        Iterator keyRingIter = pgpPub.getKeyRings();
        while (keyRingIter.hasNext())
        {
            PGPPublicKeyRing keyRing = (PGPPublicKeyRing)keyRingIter.next();

            Iterator keyIter = keyRing.getPublicKeys();
            while (keyIter.hasNext())
            {
                PGPPublicKey key = (PGPPublicKey)keyIter.next();

                if (key.isEncryptionKey())
                {
                    return key;
                }
            }
        }

        throw new IllegalArgumentException("Can't find encryption key in key ring.");
    }
    

    
    
}