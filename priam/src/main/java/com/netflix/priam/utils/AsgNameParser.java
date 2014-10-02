package com.netflix.priam.utils;

public class AsgNameParser
{
    public static class AsgNameParts
    {
        private String clusterName;
        private String dcSuffix;
        private String zone;

        AsgNameParts(String clusterName, String dcSuffix, String zone)
        {
            this.clusterName = clusterName;
            this.dcSuffix = dcSuffix;
            this.zone = zone;
        }

        public String getClusterName()
        {
            return clusterName;
        }

        public String getDcSuffix()
        {
            return dcSuffix;
        }

        public String getZone()
        {
            return zone;
        }
    }

    public static AsgNameParts parseAsgName(String asgName)
    {
        String[] nameParts = asgName.split("-");

        if (nameParts.length == 2) {
            return new AsgNameParts(nameParts[0], "", nameParts[1]);

        } else if (nameParts.length == 3) {
            return new AsgNameParts(nameParts[0], nameParts[1], nameParts[2]);
        } else {
            throw new IllegalArgumentException("Couldn't parse ASG name into it's parts");
        }
    }
}
