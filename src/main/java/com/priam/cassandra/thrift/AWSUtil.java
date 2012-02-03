package com.priam.cassandra.thrift;

import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.cassandra.config.ConfigurationException;

public class AWSUtil {
	
    public static String getDataFromUrl(String url) throws MalformedURLException, IOException, ConfigurationException
    {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        if (conn.getResponseCode() != 200)
        {
            throw new ConfigurationException("Not able to get the required Data....");
        }
        int cl = conn.getContentLength();
        byte[] b = new byte[cl];
        DataInputStream d = new DataInputStream((FilterInputStream) conn.getContent());
        d.readFully(b);
        return new String(b, "UTF-8");
    }
}
