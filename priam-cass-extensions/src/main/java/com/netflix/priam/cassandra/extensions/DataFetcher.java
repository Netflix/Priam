package com.netflix.priam.cassandra.extensions;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import com.google.common.base.Charsets;

public class DataFetcher
{
    public static String fetchData(String url)
    {
        try
        {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(1000);
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() != 200)
                throw new RuntimeException("Unable to get data for URL " + url);

            byte[] b = new byte[2048];
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataInputStream d = new DataInputStream((FilterInputStream) conn.getContent());
            int c = 0;
            while ((c = d.read(b, 0, b.length)) != -1)
                bos.write(b, 0, c);
            String return_ = new String(bos.toByteArray(), Charsets.UTF_8);
            System.out.println(String.format("Calling URL API: %s returns: %s", url, return_));
            conn.disconnect();
            return return_;
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

}
