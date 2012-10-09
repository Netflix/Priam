package com.netflix.priam.noderepair;

import com.google.inject.Inject;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.config.NodeRepairConfiguration;
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


import java.util.Map;

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
        return new NodeRepairAdapter(bundle.getJobDetail().getDescription(),cassandraConfig, amazonConfig);
    }

    public void scheduleNodeRepair(){

        Map<String, String> nodeRepairCronTime = null;
        String awsAZ = amazonConfig.getAvailabilityZone();
        String clusterName = cassandraConfig.getClusterName();

        if (clusterName.matches(".*sor_cat.*")) {
            nodeRepairCronTime = getRepairCronTime(awsAZ, "sorCat");
        } else if (clusterName.matches(".*sor_ugc.*")){
            nodeRepairCronTime = getRepairCronTime(awsAZ, "sorUgc");
        } else if (clusterName.matches(".*databus.*")){
            nodeRepairCronTime = getRepairCronTime(awsAZ, "databusCass");
        } else {
            nodeRepairCronTime = getRepairCronTime(awsAZ, "polloiCass");
        }

        //Schedule Job. * For Polloi handle differently as the keyspace name of polloi includes environment name (e.g. qa, prod) *
        if(clusterName.matches(".*polloi.*")){
            String[] words = clusterName.split("_");
            String polloiKeySpace = words[0]+"_"+words[5]+"_"+words[6]+"_"+"state";
            startScheduling(polloiKeySpace,nodeRepairCronTime.get("_default"));
        }else{
            //Schedule for each keyspace
            for(Map.Entry<String, String> entry : nodeRepairCronTime.entrySet()){
                startScheduling(entry.getKey(),entry.getValue());
            }
        }
    }

    private void startScheduling(String keyspace, String cronTime){
        JobDetail jobDetail = JobBuilder.newJob(NodeRepairAdapter.class)
                .withDescription(keyspace)
                .build();

        try {
            nodeRepairTrigger = TriggerBuilder
                    .newTrigger()
                    .withDescription(keyspace)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cronTime))
                    .build();
            scheduler.scheduleJob(jobDetail, nodeRepairTrigger);
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> getRepairCronTime(String awsAZ, String cassCluster){
        Map<String, String> nodeRepairCronTime = null;
        if(cassCluster.equals("sorCat")){
            if (awsAZ.matches("us-east-1a")) {
                nodeRepairCronTime = nodeRepairConfig.getSorCatConfiguration().getUs_east_1a();
            } else if (awsAZ.matches("us-east-1b")) {
                nodeRepairCronTime = nodeRepairConfig.getSorCatConfiguration().getUs_east_1b();
            } else if (awsAZ.matches("us-east-1c")) {
                nodeRepairCronTime = nodeRepairConfig.getSorCatConfiguration().getUs_east_1c();
            } else if (awsAZ.matches("eu-west-1a")) {
                nodeRepairCronTime = nodeRepairConfig.getSorCatConfiguration().getEu_west_1a();
            } else if (awsAZ.matches("eu-west-1b")) {
                nodeRepairCronTime = nodeRepairConfig.getSorCatConfiguration().getEu_west_1b();
            } else {
                nodeRepairCronTime = nodeRepairConfig.getSorCatConfiguration().getEu_west_1c();
            }
        } else if (cassCluster.equals("sorUgc")){
            if (awsAZ.matches("us-east-1a")) {
                nodeRepairCronTime = nodeRepairConfig.getSorUgcConfiguration().getUs_east_1a();
            } else if (awsAZ.matches("us-east-1b")) {
                nodeRepairCronTime = nodeRepairConfig.getSorUgcConfiguration().getUs_east_1b();
            } else if (awsAZ.matches("us-east-1c")) {
                nodeRepairCronTime = nodeRepairConfig.getSorUgcConfiguration().getUs_east_1c();
            } else if (awsAZ.matches("eu-west-1a")) {
                nodeRepairCronTime = nodeRepairConfig.getSorUgcConfiguration().getEu_west_1a();
            } else if (awsAZ.matches("eu-west-1b")) {
                nodeRepairCronTime = nodeRepairConfig.getSorUgcConfiguration().getEu_west_1b();
            } else {
                nodeRepairCronTime = nodeRepairConfig.getSorCatConfiguration().getEu_west_1c();
            }
        } else if (cassCluster.equals("databusCass")){
            if (awsAZ.matches("us-east-1a")) {
                nodeRepairCronTime = nodeRepairConfig.getDatabusCassConfiguration().getUs_east_1a();
            } else if (awsAZ.matches("us-east-1b")) {
                nodeRepairCronTime = nodeRepairConfig.getDatabusCassConfiguration().getUs_east_1b();
            } else if (awsAZ.matches("us-east-1c")) {
                nodeRepairCronTime = nodeRepairConfig.getDatabusCassConfiguration().getUs_east_1c();
            } else if (awsAZ.matches("eu-west-1a")) {
                nodeRepairCronTime = nodeRepairConfig.getDatabusCassConfiguration().getEu_west_1a();
            } else if (awsAZ.matches("eu-west-1b")) {
                nodeRepairCronTime = nodeRepairConfig.getDatabusCassConfiguration().getEu_west_1b();
            } else {
                nodeRepairCronTime = nodeRepairConfig.getDatabusCassConfiguration().getEu_west_1c();
            }
        } else {
            if (awsAZ.matches("us-east-1a")) {
                nodeRepairCronTime = nodeRepairConfig.getPolloiCassConfiguration().getUs_east_1a();
            } else if (awsAZ.matches("us-east-1b")) {
                nodeRepairCronTime = nodeRepairConfig.getPolloiCassConfiguration().getUs_east_1b();
            } else if (awsAZ.matches("us-east-1c")) {
                nodeRepairCronTime = nodeRepairConfig.getPolloiCassConfiguration().getUs_east_1c();
            } else if (awsAZ.matches("eu-west-1a")) {
                nodeRepairCronTime = nodeRepairConfig.getPolloiCassConfiguration().getEu_west_1a();
            } else if (awsAZ.matches("eu-west-1b")) {
                nodeRepairCronTime = nodeRepairConfig.getPolloiCassConfiguration().getEu_west_1b();
            } else {
                nodeRepairCronTime = nodeRepairConfig.getPolloiCassConfiguration().getEu_west_1c();
            }
        }
        return nodeRepairCronTime;
    }
}
