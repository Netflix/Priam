package com.netflix.priam.noderepair;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.config.NodeRepairConfiguration;
import com.netflix.priam.utils.JMXNodeTool;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;


import java.util.List;
import java.util.Map;

@Singleton
public class NodeRepairScheduler implements JobFactory {

    private final Scheduler scheduler;

    @Inject private CassandraConfiguration cassandraConfig;
    @Inject private AmazonConfiguration amazonConfig;
    @Inject private NodeRepairConfiguration nodeRepairConfig;

    private Trigger nodeRepairTrigger = null;

    public NodeRepairScheduler(){
        try {
            this.scheduler = new StdSchedulerFactory().getScheduler();
            this.scheduler.setJobFactory(this);
            scheduler.start();
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
        if (bundle.getJobDetail().getJobClass() != NodeRepairAdapter.class) {
            throw new IllegalStateException("can't schedule arbtitrary Repair");
        }
        return new NodeRepairAdapter(cassandraConfig);
    }

    public void scheduleNodeRepair() {
        String nodeRepairCronTime = null;
        String awsAZ = amazonConfig.getAvailabilityZone();

        String clusterName = cassandraConfig.getClusterName();
        if (clusterName.matches(".*sor_cat.*") || clusterName.matches(".*sor_ugc.*") || clusterName.matches(".*polloi.*")) {
            nodeRepairCronTime = cassandraConfig.getNodeRepairSorPolloi();
        } else {
            nodeRepairCronTime = cassandraConfig.getNodeRepairDatabus();
        }

        try{
            JMXNodeTool jmxNodeTool = new JMXNodeTool(cassandraConfig);
            startScheduling(nodeRepairCronTime);
        } catch (Exception e) {
            //TODO log it
        }
    }

    private void startScheduling(String cronTime){
        JobDetail jobDetail = JobBuilder.newJob(NodeRepairAdapter.class)
                .build();

        try {
            nodeRepairTrigger = TriggerBuilder
                    .newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule(cronTime))
                    .build();
            scheduler.scheduleJob(jobDetail, nodeRepairTrigger);
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }
}
