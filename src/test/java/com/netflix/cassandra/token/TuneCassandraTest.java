package com.netflix.cassandra.token;

import java.util.Properties;

import org.junit.Test;

import com.priam.conf.TuneCassandra;


public class TuneCassandraTest
{

    @Test
    public void testSaveProperties()
    {
        Properties prop = new Properties();
        prop.setProperty("Vijay", "isAwesome");        
        //TuneCassandra.saveProperties(prop, "/tmp/crap.prop");
    }
}
