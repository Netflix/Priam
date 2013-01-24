package com.netflix.priam;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.netflix.priam.identity.IMembership;
import com.netflix.priam.identity.PriamInstance;

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
    public int getRacMembershipSize()
    {
        return 3;
    }

    @Override
    public void addACL(Collection<String> listIPs, int from, int to)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void removeACL(Collection<String> listIPs, int from, int to)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public List<String> listACL(int from, int to)
    {
        // TODO Auto-generated method stub
        return null;
    }

    public List<PriamInstance> getAllInstances(String appName)
    {
        return new ArrayList<PriamInstance>(0);
    }

    public PriamInstance getThisInstance()
    {
        return new PriamInstance("localhost", "ami-1234abcd", "us-east-1", "us-east-1a", "127.0.0.1");
    }
}
