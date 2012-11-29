package com.netflix.priam.cassandra.extensions;

import java.lang.instrument.Instrumentation;

/**
 * A <a href="http://docs.oracle.com/javase/6/docs/api/java/lang/instrument/package-summary.html">PreMain</a> class
 * to run inside of the cassandra process. Contacts Priam for essential cassandra startup information
 * like token and seeds.
 */
public class PriamStartupAgent
{
    public static void premain(String agentArgs, Instrumentation inst)
    {
        PriamStartupAgent agent = new PriamStartupAgent();
        agent.setPriamProperties();
    }

    private void setPriamProperties()
    {
        String token = null;
        String seeds = null;
        boolean isReplace = false;
        while (true)
        {
            try
            {
                token = DataFetcher.fetchData("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_token");
                seeds = DataFetcher.fetchData("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_seeds");
                isReplace = Boolean.parseBoolean(DataFetcher.fetchData("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/is_replace_token"));
            }
            catch (Exception e)
            {
                System.out.println("Failed to obtain startup data from priam, can not start yet. will retry shortly");
                e.printStackTrace();
            }
  
            if (token != null && seeds != null)
                break;
            try
            {
                Thread.sleep(5 * 1000);
            }
            catch (InterruptedException e1)
            {
                // do nothing.
            }
        }
        
        System.setProperty("cassandra.initial_token", token);
  
        if (isReplace)
            System.setProperty("cassandra.replace_token", token);
    }

}
