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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import javax.management.remote.JMXConnector;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;


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
                throw new ConfigurationException("Unable to get data for URL " + url);
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
            byte[] digest = Files.getDigest(file, MessageDigest.getInstance("MD5"));
            return toHex(digest);
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

    /**
     * copy the input to the output.
     */
    public static void copyAndClose(InputStream input, OutputStream output) throws IOException
    {
        try
        {
            IOUtils.copy(input, output);
        }
        finally
        {
            IOUtils.closeQuietly(input);
            IOUtils.closeQuietly(output);
        }
    }

    public static File[] sortByLastModifiedTime(File[] files)
    {
        Arrays.sort(files, new Comparator<File>()
        {
            public int compare(File file1, File file2)
            {
                return Long.valueOf(file2.lastModified()).compareTo(Long.valueOf(file1.lastModified()));
            }
        });
        return files;
    }

    public static void closeQuietly(JMXNodeTool tool)
    {
        try
        {
            tool.close();
        }
        catch (IOException e)
        {
            // Do nothing.
        }
    }

    public static <T> T retryForEver(RetryableCallable<T> retryableCallable)
    {
        try
        {
            retryableCallable.set(Integer.MAX_VALUE, 1 * 1000);
            return retryableCallable.call();
        }
        catch (Exception e)
        {
            // this might not happen because we are trying Integer.MAX_VALUE
            // times.
        }
        return null;
    }

    public static void closeQuietly(JMXConnector jmc)
    {
        try
        {
            jmc.close();
        }
        catch (IOException e)
        {
            // Do nothing.
        }
    }

    public static Date getDayBeginTime(Date date)
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTime(date);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    public static Date getDayEndTime(Date date)
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTime(date);
        cal.set(Calendar.HOUR, 23);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }
}
