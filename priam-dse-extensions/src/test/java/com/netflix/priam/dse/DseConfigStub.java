package com.netflix.priam.dse;

import java.util.HashSet;
import java.util.Set;

public class DseConfigStub implements IDseConfiguration
{
    boolean auditLogEnabled;

    public String getDseYamlLocation()
    {
        return null;
    }

    public String getDseDelegatingSnitch()
    {
        return null;
    }

    public NodeType getNodeType()
    {
        return null;
    }

    public boolean isAuditLogEnabled()
    {
        return auditLogEnabled;
    }

    public void setAuditLogEnabled(boolean b)
    {
        auditLogEnabled = b;
    }

    public String getAuditLogExemptKeyspaces()
    {
        return "YourSwellKeyspace";
    }

    public Set<AuditLogCategory> getAuditLogCategories()
    {
        return new HashSet<AuditLogCategory>(){{ this.add(AuditLogCategory.ALL); }};
    }
}
