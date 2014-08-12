package com.netflix.priam.resources;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileReader;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class CassandraAdminTest
{

    
    @Test
    public void parseGossipInfo() throws Exception
    {
    	BufferedReader reader = new BufferedReader(new FileReader("src/test/resources/gossipinfo.txt"));
    	String line = null;
    	String result = "";
    	while ((line = reader.readLine()) != null) {
    	    result += line + "\n";
    	}
    	reader.close();
    	
    	JSONObject rootObj = CassandraAdmin.parseGossipInfo(result);
    	
        try {
        	rootObj.get("ip-10-75-23-21.eu-west-1.compute.internal");
        	fail("should not be found");
        } catch (JSONException notFound){
        	
        }
        Assert.assertNotNull(rootObj.get("10.80.147.144"));
        Assert.assertNotNull(rootObj.get("10.80.147.20"));
        Assert.assertNotNull(rootObj.get("10.75.23.21"));
        Assert.assertTrue(rootObj.getJSONObject("10.80.147.144").get("  SCHEMA").equals("bbb09269-73c9-36a4-b656-13470aa8a3fd"));
        Assert.assertTrue(rootObj.getJSONObject("10.75.23.21").get("  SCHEMA").equals("bbb09269-73c9-36a4-b656-13470aa8a3fd"));
        Assert.assertTrue(rootObj.getJSONObject("10.80.147.144").get("  RPC_ADDRESS").equals("10.80.147.144"));
        Assert.assertTrue(rootObj.getJSONObject("10.80.147.20").get("  RPC_ADDRESS").equals("10.80.147.20"));
        Assert.assertTrue(rootObj.getJSONObject("10.75.23.21").get("  RPC_ADDRESS").equals("10.75.23.21"));
        Assert.assertEquals(rootObj.length(), 3);
    	
    }
    
}
