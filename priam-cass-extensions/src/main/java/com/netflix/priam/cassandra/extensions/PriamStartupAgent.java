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
package com.netflix.priam.cassandra.extensions;

import java.lang.instrument.Instrumentation;


import org.apache.cassandra.utils.FBUtilities;

/**
 * A <a href="http://docs.oracle.com/javase/6/docs/api/java/lang/instrument/package-summary.html">PreMain</a> class
 * to run inside of the cassandra process. Contacts Priam for essential cassandra startup information
 * like token and seeds.
 */
public class PriamStartupAgent
{
	public static String REPLACED_ADDRESS_MIN_VER = "1.2.11";
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
        String replacedIp = "";
        
        while (true)
        {
            try
            {
                token = DataFetcher.fetchData("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_token");
                seeds = DataFetcher.fetchData("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_seeds");
                isReplace = Boolean.parseBoolean(DataFetcher.fetchData("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/is_replace_token"));
                replacedIp = DataFetcher.fetchData("http://127.0.0.1:8080/Priam/REST/v1/cassconfig/get_replaced_ip");
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
        {	
        	System.out.println("Detect cassandra version : " + FBUtilities.getReleaseVersionString());
        	if (FBUtilities.getReleaseVersionString().compareTo(REPLACED_ADDRESS_MIN_VER) < 0)
        	{
        		System.setProperty("cassandra.replace_token", token);
        	} else 
        	{	
               System.setProperty("cassandra.replace_address", replacedIp);
        	}
        }

    }

}
