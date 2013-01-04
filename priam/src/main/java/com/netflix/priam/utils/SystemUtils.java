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
import java.io.DataInputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import javax.management.remote.JMXConnector;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.netflix.priam.IConfiguration;


public class SystemUtils
{
    private static final Logger logger = LoggerFactory.getLogger(SystemUtils.class);
    private static final String SUDO_STRING = "/usr/bin/sudo";
    private static final int SCRIPT_EXECUTE_WAIT_TIME_MS = 1000;

    /**
     * Start Cassandra process from this co-process.
     */
    public static void startCassandra(boolean join_ring, IConfiguration config) throws IOException, InterruptedException
    {
        logger.info("Starting cassandra server ....Join ring=" + join_ring);

        List<String> command = Lists.newArrayList();
        if (!"root".equals(System.getProperty("user.name")))
        {
            command.add(SUDO_STRING);
            command.add("-n");
            command.add("-E");
        }
        for(String param : config.getCassStartupScript().split(" ")){
            if( StringUtils.isNotBlank(param))
                command.add(param);
        }
        ProcessBuilder startCass = new ProcessBuilder(command);
        Map<String, String> env = startCass.environment();
        env.put("HEAP_NEWSIZE", config.getHeapNewSize());
        env.put("MAX_HEAP_SIZE", config.getHeapSize());
        env.put("DATA_DIR", config.getDataFileLocation());
        env.put("COMMIT_LOG_DIR", config.getCommitLogLocation());
        env.put("LOCAL_BACKUP_DIR", config.getBackupLocation());
        env.put("CACHE_DIR", config.getCacheLocation());
        env.put("JMX_PORT", "" + config.getJmxPort());
        env.put("MAX_DIRECT_MEMORY", config.getMaxDirectMemory());
        env.put("cassandra.join_ring", join_ring ? "true" : "false");
        startCass.directory(new File("/"));
        startCass.redirectErrorStream(true);
        Process starter = startCass.start();
        logger.info("Starting cassandra server ....");
        Thread.sleep(SCRIPT_EXECUTE_WAIT_TIME_MS);
        try
        {
            int code = starter.exitValue();
            if (code == 0)
                logger.info("Cassandra server has been started");
            else
            {
                logger.error("Unable to start cassandra server. Error code: {}", code);
                logProcessOutput(starter);
            }
        } catch (IllegalThreadStateException e)
        {
        }
    }

    /**
     * Stop Cassandra process from this co-process.
     */
    public static void stopCassandra(IConfiguration config) throws IOException, InterruptedException
    {
        logger.info("Stopping cassandra server ....");
        List<String> command = Lists.newArrayList();
        if (!"root".equals(System.getProperty("user.name")))
        {
            command.add(SUDO_STRING);
            command.add("-n");
            command.add("-E");
        }
        for(String param : config.getCassStopScript().split(" ")){
            if( StringUtils.isNotBlank(param))
                command.add(param);
        }
        ProcessBuilder stopCass = new ProcessBuilder(command);
        stopCass.directory(new File("/"));
        stopCass.redirectErrorStream(true);
        Process stopper = stopCass.start();
        Thread.sleep(SCRIPT_EXECUTE_WAIT_TIME_MS);
        try
        {
            int code = stopper.exitValue();
            if (code == 0)
                logger.info("Cassandra server has been stopped");
            else
            {
                logger.error("Unable to stop cassandra server. Error code: {}", code);
                logProcessOutput(stopper);
            }
        } catch (IllegalThreadStateException e)
        {
        }
    }

    static void logProcessOutput(Process p) throws IOException
    {
        final String stdOut = readProcessStream(p.getInputStream());
        final String stdErr = readProcessStream(p.getErrorStream());
        logger.info("std_out: {}", stdOut);
        logger.info("std_err: {}", stdErr);
    }

    private static String readProcessStream(InputStream inputStream) throws IOException
    {
        final byte[] buffer = new byte[512];
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(buffer.length);
        int cnt;
        while ((cnt = inputStream.read(buffer)) != -1)
            baos.write(buffer, 0, cnt);
        return baos.toString();
    }

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
