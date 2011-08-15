package com.priam.identity;

import java.util.Collection;
import java.util.List;

public interface IMembership
{
    public List<String> getRacMembership();

    public int getRacMembershipSize();

    public int getRacCount();

    public void addACL(Collection<String> listIPs, int from, int to);

    public void removeACL(Collection<String> listIPs, int from, int to);

    public List<String> listACL();
}