package com.netflix.priam;

import java.util.List;

import com.netflix.priam.identity.IMembership;

public class FakeMembership implements IMembership
{

    private List<String> instances;

    public FakeMembership(List<String> priamInstances)
    {
        this.instances = priamInstances;
    }
    
    public void setInstances( List<String> priamInstances)
    {
        this.instances = priamInstances;
    }

    @Override
    public List<String> getAutoScaleGroupMembership()
    {
        return instances;
    }

    @Override
    public int getAvailabilityZoneMembershipSize()
    {
        return 3;
    }

    @Override
    public int getUsableAvailabilityZones()
    {
        return 3;
    }

    @Override
    public void expandAvailabilityZoneMembership(int count)
    {
        // TODO Auto-generated method stub
        
    }
}
