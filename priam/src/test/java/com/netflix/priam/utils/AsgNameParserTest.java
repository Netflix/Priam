package com.netflix.priam.utils;

import junit.framework.Assert;
import org.junit.Test;

public class AsgNameParserTest
{
    @Test
    public void withDcSuffix()
    {
        AsgNameParser.AsgNameParts parts = AsgNameParser.parseAsgName("cass_1-_solr-useast1a");
        Assert.assertEquals(parts.getClusterName(), "cass_1");
        Assert.assertEquals(parts.getDcSuffix(), "_solr");
        Assert.assertEquals(parts.getZone(), "useast1a");
    }

    @Test
    public void withoutDcSuffix()
    {
        AsgNameParser.AsgNameParts parts = AsgNameParser.parseAsgName("cass_1-useast1a");
        Assert.assertEquals(parts.getClusterName(), "cass_1");
        Assert.assertEquals(parts.getDcSuffix(), "");
        Assert.assertEquals(parts.getZone(), "useast1a");
    }
}
