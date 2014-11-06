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
        Assert.assertNotNull(rootObj.get("10.80.147.20"));
    	
    }
    
}
