package com.netflix.priam.noderepair;

import com.bazaarvoice.zookeeper.ZooKeeperConnection;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

@Singleton
public final class NodeRepairScheduler implements JobFactory {

    private Scheduler scheduler;
    private final static Logger logger = LoggerFactory.getLogger(NodeRepairScheduler.class);

    @Inject private CassandraConfiguration cassandraConfig;
    @Inject private AmazonConfiguration amazonConfig;
    @Inject private Optional<ZooKeeperConnection> zooKeeperConnection;

    private Trigger nodeRepairTrigger = null;

    public NodeRepairScheduler(){
    }

    @Override
    public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
        if (bundle.getJobDetail().getJobClass() != NodeRepairAdapter.class) {
            throw new IllegalStateException("can't schedule arbtitrary Repair");
        }
        return new NodeRepairAdapter(cassandraConfig, zooKeeperConnection, amazonConfig);
    }

    public synchronized void scheduleNodeRepair() {
        String nodeRepairCronTime = null;

        String clusterName = cassandraConfig.getClusterName();
        if (clusterName.matches(".*sor_cat.*") || clusterName.matches(".*sor_ugc.*") || clusterName.matches(".*polloi.*")) {
            nodeRepairCronTime = cassandraConfig.getNodeRepairSorPolloi();
        } else {
            nodeRepairCronTime = cassandraConfig.getNodeRepairDatabus();
        }
        logger.info("nodeRepairCronTime is {}", nodeRepairCronTime);

        try{
            logger.info("node repair scheduling start");
            startScheduling(nodeRepairCronTime);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
            logger.info("node repair scheduling done");
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    public void setJobFactory(){
        try {
            Properties properties = new Properties();
            properties.setProperty("org.quartz.scheduler.instanceName","nodeRepairScheduler");
            properties.setProperty("org.quartz.threadPool.threadCount","10");
            properties.setProperty("org.quartz.jobStore.class","org.quartz.simpl.RAMJobStore");

            this.scheduler = new StdSchedulerFactory(properties).getScheduler();
            this.scheduler.setJobFactory(this);
            scheduler.start();
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }

    }
}
