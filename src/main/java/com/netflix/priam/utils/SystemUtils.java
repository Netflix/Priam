package com.netflix.priam.utils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
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

    /**
     * Start cassandra process from this co-process.
     */
    public static void startCassandra(boolean join_ring, IConfiguration config) throws IOException, InterruptedException
    {
        logger.info("Starting cassandra server ....Join ring=" + join_ring);

        List<String> command = Lists.newArrayList();
        if (!"root".equals(System.getProperty("user.name")))
        {
            command.add(SUDO_STRING);
            command.add("-E");
        }
        command.add(config.getCassStartupScript());
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
        startCass.start();
        logger.info("Starting cassandra server ....");
    }

    /**
     * Stop cassandra process from this co-process.
     * 
     * @throws IOException
     */
    public static void stopCassandra(IConfiguration config) throws IOException
    {
        logger.info("Stopping cassandra server ....");
        List<String> command = Lists.newArrayList();
        if (!"root".equals(System.getProperty("user.name")))
        {
            command.add(SUDO_STRING);
            command.add("-E");
        }
        command.add(config.getCassStopScript());
        ProcessBuilder stopCass = new ProcessBuilder(command);
        stopCass.start();
    }

    public static String getDataFromUrl(String url)
    {
        try
        {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
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

    public static int runSysCommand(String command) throws InterruptedException, IOException
    {
        logger.info("Running command " + command.toString());

        Process p = Runtime.getRuntime().exec(command.toString());

        StreamReader stdError = new StreamReader(p.getErrorStream(), StreamReader.ERROR);
        StreamReader stdInput = new StreamReader(p.getInputStream(), StreamReader.INFO);
        stdError.start();
        stdInput.start();

        int exitVal = p.waitFor();
        logger.info("Done sys command Exitval: " + exitVal);
        return exitVal;
    }

    /**
     * delete all the files/dirs in the given Directory but dont delete the dir
     * itself.
     * 
     * @throws IOException
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

    /**
     * Create a hash of the String which will be an absolute value...
     */
    public static int hash(String string)
    {
        int hash = string.hashCode();
        return Math.abs(hash);
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

    public static class StreamReader extends Thread
    {
        public static final String ERROR = "ERROR";
        public static final String INFO = "INFO";
        private StringBuilder output = new StringBuilder();
        private InputStream is;
        private String type;

        public StreamReader(InputStream is, String type)
        {
            this.is = is;
            this.type = type;
        }

        public void run()
        {
            try
            {
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line = null;
                while ((line = br.readLine()) != null)
                {
                    output.append(line + "\n");
                    if (type == INFO)
                        logger.info(line);
                    else
                        logger.error(line);
                }
                br.close();
                logger.info("Done running sys command" + output.toString());
            }
            catch (IOException ioe)
            {
                logger.error("Error in running sys command: ", ioe);
            }
        }

        public String getOutput()
        {
            return output.toString();
        }
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
