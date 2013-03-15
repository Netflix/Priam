package com.netflix.priam;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.defaultimpl.PriamConfiguration;

@Singleton
public class FakeConfiguration implements IConfiguration
{

	public static final String FAKE_REGION = "us-east-1";

    public String region;
    public String appName;
    public String zone;
    public String instance_id;
    public String restorePrefix;

    public FakeConfiguration(String region, String appName, String zone, String ins_id)
    {
        this.region = region;
        this.appName = appName;
        this.zone = zone;
        this.instance_id = ins_id;
        this.restorePrefix  = "";
    }

    @Override
    public void intialize()
    {
        // TODO Auto-generated method stub

    }

    @Override
    public String getBackupLocation()
    {
        // TODO Auto-generated method stub
        return "casstestbackup";
    }

    @Override
    public String getBackupPrefix()
    {
        // TODO Auto-generated method stub
        return "TEST-netflix.platform.S3";
    }

    @Override
    public boolean isCommitLogBackup()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getCommitLogLocation()
    {
        // TODO Auto-generated method stub
        return "cass/commitlog";
    }

    @Override
    public String getDataFileLocation()
    {
        // TODO Auto-generated method stub
        return "target/data";
    }

    @Override
    public String getCacheLocation()
    {
        // TODO Auto-generated method stub
        return "cass/caches";
    }

    @Override
    public List<String> getRacs()
    {
        return Arrays.asList("az1", "az2", "az3");
    }

    @Override
    public int getJmxPort()
    {
        return 7199;
    }

    @Override
    public int getThriftPort()
    {
        return 9160;
    }

    @Override
    public String getSnitch()
    {
        return "org.apache.cassandra.locator.SimpleSnitch";
    }

    @Override
    public String getRac()
    {
        return this.zone;
    }

    @Override
    public String getHostname()
    {
        // TODO Auto-generated method stub
        return instance_id;
    }

    @Override
    public String getInstanceName()
    {
        return instance_id;
    }

    @Override
    public String getHeapSize()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getHeapNewSize()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getBackupHour()
    {
        // TODO Auto-generated method stub
        return 12;
    }

    @Override
    public String getRestoreSnapshot()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getAppName()
    {
        return appName;
    }

    @Override
    public String getACLGroupName()
    {
        return this.getAppName();
    }

    @Override
    public int getMaxBackupUploadThreads()
    {
        // TODO Auto-generated method stub
        return 2;
    }

    @Override
    public String getDC()
    {
        // TODO Auto-generated method stub
        return this.region;
    }

    @Override
    public int getMaxBackupDownloadThreads()
    {
        // TODO Auto-generated method stub
        return 3;
    }

    public void setRestorePrefix(String prefix)
    {
        // TODO Auto-generated method stub
        restorePrefix = prefix;
    }

    @Override
    public String getRestorePrefix()
    {
        // TODO Auto-generated method stub
        return restorePrefix;
    }

    @Override
    public String getBackupCommitLogLocation()
    {
        return "cass/backup/cl/";
    }

    @Override
    public boolean isMultiDC()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getASGName()
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public boolean isIncrBackup()
    {
        return true;
    }

    @Override
    public String getHostIP()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int getUploadThrottle()
    {
        // TODO Auto-generated method stub
        return 0;
    }

	@Override
	public boolean isLocalBootstrapEnabled() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int getInMemoryCompactionLimit() {
		return 8;
	}

	@Override
	public int getCompactionThroughput() {
		// TODO Auto-generated method stub
		return 0;
	}

    @Override
    public String getMaxDirectMemory()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getBootClusterName()
    {
        // TODO Auto-generated method stub
        return "cass_bootstrap";
    }

    @Override
    public String getCassHome()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getCassStartupScript()
    {
        // TODO Auto-generated method stub
        return "true";
    }

    @Override
    public List<String> getRestoreKeySpaces()
    {
        // TODO Auto-generated method stub
        return Lists.newArrayList();
    }

    @Override
    public long getBackupChunkSize()
    {        
        return 5L*1024*1024;
    }

    @Override
    public void setDC(String region)
    {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean isRestoreClosestToken()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getCassStopScript()
    {
        return "true";
    }

    @Override
    public int getStoragePort()
    {
        return 7101;
    }

    @Override
    public String getSeedProviderName()
    {
        return "org.apache.cassandra.locator.SimpleSeedProvider";
    }

    @Override
    public int getBackupRetentionDays()
    {
        return 5;
    }

    @Override
    public List<String> getBackupRacs()
    {
        return Lists.newArrayList();
    }
    
    public int getMaxHintWindowInMS()
    {
        return 36000;
    }

    @Override
    public int getHintHandoffDelay()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMemtableTotalSpaceMB()
    {
        return 0;
    }

    @Override
    public int getStreamingThroughputMB()
    {
        return 400;
    }

    @Override
    public boolean getMultithreadedCompaction()
    {
        return false;
    }

    public String getPartitioner()
    {
        return "org.apache.cassandra.dht.RandomPartitioner";
    }

    @Override
    public int getSSLStoragePort()
    {
        // TODO Auto-generated method stub
        return 7103;
    }

    public String getKeyCacheSizeInMB()
    {
        return "16";
    }

    public String getKeyCacheKeysToSave()
    {
        return "32";
    }

    public String getRowCacheSizeInMB()
    {
        return "4";
    }

    public String getRowCacheKeysToSave()
    {
        return "4";
    }

	@Override
	public String getCassProcessName() {
		return "CassandraDaemon";
	}

    public String getYamlLocation()
    {
        return "conf/cassandra.yaml";
    }

    public String getAuthenticator()
    {
        return PriamConfiguration.DEFAULT_AUTHENTICATOR;
    }

    public String getAuthorizer()
    {
        return PriamConfiguration.DEFAULT_AUTHORIZER;
    }
}
