package com.netflix.priam;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

    public FakeConfiguration()
    {
        this(FAKE_REGION, "my_fake_cluster", "my_zone", "i-01234567");
    }

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
    public int getNativeTransportPort()
    {
        return 9042;
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
        return "/tmp/priam";
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

    public int getHintedHandoffThrottleKb()
    {
        return 1024;
    }

    public int getMaxHintThreads()
    {
        return 1;
    }

    @Override
    public int getMemtableTotalSpaceMB()
    {
        return 0;
    }

    /**
     * @return memtable_cleanup_threshold in C* yaml
     */
    @Override
    public double getMemtableCleanupThreshold() {
        return 0.11;
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

    public int getNumTokens()
    {
        return 1;
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

    @Override
	public String getTargetKSName() {
		return null;
	}

	@Override
	public String getTargetCFName() {
		return null;
	}

	@Override
	public boolean doesCassandraStartManually() {
		return false;
	}

    @Override
    public boolean isVpcRing() {
        return false;
    }

    public String getInternodeCompression()
    {
        return "all";
    }

    @Override
    public boolean isBackingUpCommitLogs()
    {
        return false;
    }

    @Override
    public String getCommitLogBackupArchiveCmd()
    {
        return null;
    }

    @Override
    public String getCommitLogBackupRestoreCmd()
    {
        return null;
    }

    @Override
    public String getCommitLogBackupRestoreFromDirs()
    {
        return null;
    }

    @Override
    public String getCommitLogBackupRestorePointInTime()
    {
        return null;
    }

    public void setRestoreKeySpaces(List<String> keyspaces) {
            
    }

    @Override
    public int maxCommitLogsRestore() {		
       return 0;
    }

    public boolean isClientSslEnabled()
    {
        return true;
    }

    public String getInternodeEncryption()
    {
        return "all";
    }

    public boolean isDynamicSnitchEnabled()
    {
        return true;
    }

    public boolean isThriftEnabled()
    {
        return true;
    }

    public boolean isNativeTransportEnabled()
    {
        return false;
    }
	
    @Override
    public String getS3EndPoint() {
	return "s3-external-1.amazonaws.com";
    }

    public int getConcurrentReadsCnt()
    {
        return 8;
    }

    public int getConcurrentWritesCnt()
    {
        return 8;
    }

    public int getConcurrentCompactorsCnt()
    {
        return 1;
    }

	@Override
	public String getRpcServerType() {
		return "hsha";
	}

    @Override
    public int getRpcMinThreads() {
        return 16;
    }

    @Override
    public int getRpcMaxThreads() {
        return 2048;
    }

	@Override
	public int getIndexInterval() {
		return 0;
	}
	
	public String getExtraConfigParams() {
		return null;
	}
	
    public String getCassYamlVal(String priamKey) {
    	return "";
    }

    @Override
    public boolean getAutoBoostrap() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getDseClusterType() {
        // TODO Auto-generated method stub
        return "cassandra";
    }

	@Override
	public boolean isCreateNewTokenEnable() {
		return true;  //allow Junit test to create new tokens
	}

	@Override
	public String getPrivateKeyLocation() {
		return null;
	}

	@Override
	public String getRestoreSourceType() {
		return null;
	}

	@Override
	public boolean isEncryptBackupEnabled() {
		return false;
	}

	@Override
	public String getAWSRoleAssumptionArn() {
		return "";
	}
	
    @Override
	public String getClassicEC2RoleAssumptionArn() {
		return null;
	}

    @Override
	public String getVpcEC2RoleAssumptionArn() {
		return null;
	}

    @Override
	public boolean isDualAccount(){
    	return false;
    }

	@Override
	public String getGcsServiceAccountId() {
		return null;
	}

	@Override
	public String getGcsServiceAccountPrivateKeyLoc() {
		return null;
	}

	@Override
	public String getPgpPasswordPhrase() {
		return null;
	}

	@Override
	public String getPgpPublicKeyLoc() {
		return null;
	}

    /**
     * Use this method for adding extra/ dynamic cassandra startup options or env properties
     *
     * @return
     */
    @Override
    public Map<String, String> getExtraEnvParams() {
        return null;
    }

    @Override
    public String getRestoreKeyspaceFilter()
    {
        return null;
    }
	
    @Override
    public String getRestoreCFFilter() {
    	return null;
    }
    
   @Override
    public String getIncrementalKeyspaceFilters() {
    	return null;
    }
   
    @Override
    public String getIncrementalCFFilter() {
    	return null;
    }
    
    @Override
    public String getSnapshotKeyspaceFilters() {
    	return null;
    }
    
    @Override
    public String getSnapshotCFFilter() {
    	return null;
    }
    
    @Override
    public String getVpcId() {
    	return "";
    }

	@Override
	public Boolean isIncrBackupParallelEnabled() {
		return false;
	}

	@Override
	public int getIncrementalBkupMaxConsumers() {
		return 2;
	}

	@Override
	public int getUncrementalBkupQueueSize() {
		return 100;
	}

    /**
     * @return tombstone_warn_threshold in yaml
     */
    @Override
    public int getTombstoneWarnThreshold() {
        return 1000;
    }

    /**
     * @return tombstone_failure_threshold in yaml
     */
    @Override
    public int getTombstoneFailureThreshold() {
        return 100000;
    }

    /**
     * @return streaming_socket_timeout_in_ms in yaml
     */
    @Override
    public int getStreamingSocketTimeoutInMS() {
        return 86400000;
    }
}
