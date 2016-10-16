/**
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.priam.utils;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.remote.JMXConnector;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.util.Date;
import java.util.List;


public class SystemUtils
{
    private static final Logger logger = LoggerFactory.getLogger(SystemUtils.class);

    public static String getDataFromUrl(String url)
    {
        try
        {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(1000);
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() != 200)
            {
                throw new RuntimeException("Unable to get data for URL " + url);
            }
            byte[] b = new byte[2048];
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataInputStream d = new DataInputStream((FilterInputStream) conn.getContent());
            int c = 0;
            while ((c = d.read(b, 0, b.length)) != -1)
                bos.write(b, 0, c);
            String return_ = new String(bos.toByteArray(), Charsets.UTF_8);
            logger.info("Calling URL API: {} returns: {}", url, return_);
            conn.disconnect();
            return return_;
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }

    }
    
    /*
     * @param datae to format
     * @param e.g. yyyymmddhhmm
     * @return formatted date
     */
    public static String formatDate(Date date, String format) {
		String s = new DateTime(date).toString(format);
		return s;
    }

    /**
     * delete all the files/dirs in the given Directory but dont delete the dir
     * itself.
     */
    public static void cleanupDir(String dirPath, List<String> childdirs) throws IOException
    {
        if (childdirs == null || childdirs.size() == 0)
            FileUtils.cleanDirectory(new File(dirPath));
        else
        {
            for (String cdir : childdirs)
                FileUtils.cleanDirectory(new File(dirPath + "/" + cdir));
        }
    }

    public static void createDirs(String location)
    {
        File dirFile = new File(location);
        if (dirFile.exists() && dirFile.isFile())
        {
            dirFile.delete();
            dirFile.mkdirs();
        }
        else if (!dirFile.exists())
            dirFile.mkdirs();
    }

    public static byte[] md5(byte[] buf)
    {
        try
        {
            MessageDigest mdigest = MessageDigest.getInstance("MD5");
            mdigest.update(buf, 0, buf.length);
            return mdigest.digest();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a Md5 string which is similar to OS Md5sum
     */
    public static String md5(File file)
    {
        try
        {
            HashCode hc = Files.hash(file, Hashing.md5());
            return toHex(hc.asBytes());
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static String toHex(byte[] digest)
    {
        StringBuffer sb = new StringBuffer(digest.length * 2);
        for (int i = 0; i < digest.length; i++)
        {
            String hex = Integer.toHexString(digest[i]);
            if (hex.length() == 1)
            {
                sb.append("0");
            }
            else if (hex.length() == 8)
            {
                hex = hex.substring(6);
            }
            sb.append(hex);
        }
        return sb.toString().toLowerCase();
    }

    public static String toBase64(byte[] md5)
    {
        byte encoded[] = Base64.encodeBase64(md5, false);
        return new String(encoded);
    }

    public static void closeQuietly(JMXNodeTool tool)
    {
        try
        {
            tool.close();
        }
        catch (Exception e)
        {
            logger.warn("failed to close jxm node tool", e);
        }
       
    }

    public static void closeQuietly(JMXConnector jmc)
    {
        try
        {
            jmc.close();
        }
        catch (Exception e)
        {
            logger.warn("failed to close JMXConnectorMgr", e);
        }
    }   
    
}
