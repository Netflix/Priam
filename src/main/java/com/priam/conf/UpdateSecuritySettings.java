package com.priam.conf;

import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.priam.identity.IMembership;
import com.priam.identity.IPriamInstanceFactory;
import com.priam.identity.PriamInstance;
import com.priam.scheduler.SimpleTimer;
import com.priam.scheduler.Task;
import com.priam.scheduler.TaskTimer;

/**
 * this class will associate an Public IP's with a new instance so they can talk
 * across the regions.
 * 
 * Requirement: 1) Nodes in the same region needs to be able to talk to each
 * other. 2) Nodes in other regions needs to be able to talk to the others in
 * the other region.
 * 
 * Assumption: 1) IPriamInstanceFactory will provide the membership... and will
 * be visible across the regions 2) IMembership amazon or any other
 * implementation which can tell if the instance is part of the group (ASG in
 * amazons case).
 * 
 * @author "Vijay Parthasarathy"
 */
@Singleton
public class UpdateSecuritySettings extends Task
{
    public static final String JOBNAME = "Update_SG";
    private static Random ran = new Random();
    private IPriamInstanceFactory factory;
    private IConfiguration config;
    private IMembership membership;
    public static boolean firstTimeUpdated = false;

    @Inject
    public UpdateSecuritySettings(IPriamInstanceFactory factory, IConfiguration config, IMembership membership)
    {
        this.factory = factory;
        this.config = config;
        this.membership = membership;        
    }

    /**
     * This is called when a node boots up.... in addition seeds in other region
     * call it frequently between 60 - 120 Secs....
     * 
     * Seeds in cassandra are the first node in each Availablity Zone....
     */
    @Override
    public void execute()
    {
        // if seed dont execute.
        List<String> acls = membership.listACL();
        List<PriamInstance> instances = factory.getAllIds(config.getAppName());

        // iterate to add...
        List<String> add = Lists.newArrayList();
        for (PriamInstance instance : factory.getAllIds(config.getAppName()))
        {
            String range = instance.getHostIP() + "/32";
            if (!acls.contains(range))
                add.add(range);
        }
        if (add.size() > 0){
            membership.addACL(add, 7103, 7103);
            firstTimeUpdated = true;
        }

        // just iterate to generate ranges.
        List<String> currentRanges = Lists.newArrayList();
        for (PriamInstance instance : instances)
        {
            String range = instance.getHostIP() + "/32";
            currentRanges.add(range);
        }

        // iterate to remove...
        List<String> remove = Lists.newArrayList();
        for (String acl : acls)
            if (!currentRanges.contains(acl)) // if not found then remove....
                remove.add(acl);
        if (remove.size() > 0){
            membership.removeACL(remove, 7103, 7103);
            firstTimeUpdated = true;
        }
    }

    public static TaskTimer getTimer()
    {
        SimpleTimer return_;
        if (PriamServer.instance.id.isSeed())
            return_ = new SimpleTimer(JOBNAME, 120 * 1000 + ran.nextInt(120 * 1000));
        else
            return_ = new SimpleTimer(JOBNAME);
        return return_;
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }
}
