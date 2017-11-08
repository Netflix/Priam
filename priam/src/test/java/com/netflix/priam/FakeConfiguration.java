/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.priam;

import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.inject.Singleton;
import com.netflix.priam.tuner.JVMOption;
import com.netflix.priam.defaultimpl.PriamConfiguration;
import com.netflix.priam.tuner.GCType;
import com.netflix.priam.identity.config.InstanceDataRetriever;
import com.netflix.priam.identity.config.LocalInstanceDataRetriever;
import com.netflix.priam.scheduler.SchedulerType;
import com.netflix.priam.scheduler.UnsupportedTypeException;
import org.apache.commons.lang3.StringUtils;

@Singleton
public class FakeConfiguration implements IConfiguration
{

	public static final String FAKE_REGION = "us-east-1";

    public String region;
    public String appName;
    public String zone;
    public String instance_id;
    public String restorePrefix;
    public int numTokens;

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
        this.numTokens = 1;
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
    public String getLogDirLocation() {
        return null;
    }

    @Override
    public String getHintsLocation() {
        return "target/hints";
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

    /**
     * @return Enables Remote JMX connections n C*
     */
    @Override
    public boolean enableRemoteJMX() {
        return false;
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
    public String getBackupCronExpression() {
        return null;
    }

    @Override
    public SchedulerType getBackupSchedulerType() {
        return SchedulerType.HOUR;
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

    /**
     * Amazon specific setting to query Additional/ Sibling ASG Memberships in csv format to consider while calculating RAC membership
     */
    @Override
    public String getSiblingASGNames() {
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
    public InstanceDataRetriever getInstanceDataRetriever() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        return new LocalInstanceDataRetriever();
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
        return "/usr/bin/false";
    }

    @Override
    public List<String> getRestoreKeySpaces()
    {
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
        return numTokens;
    }

    public String getYamlLocation()
    {
        return "conf/cassandra.yaml";
    }

    @Override
    public String getJVMOptionsFileLocation() {
        return "src/test/resources/conf/jvm.options";
    }

    @Override
    public GCType getGCType() throws UnsupportedTypeException {
        return GCType.CMS;
    }

    @Override
    public Map<String, JVMOption> getJVMExcludeSet() {
        return null;
    }

    @Override
    public Map<String, JVMOption> getJVMUpsertSet() {
        return null;
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
    public String getCommitLogBackupPropsFile()
    {
        return getCassHome() + PriamConfiguration.DEFAULT_COMMITLOG_PROPS_FILE;
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

    @Override
    public int getCompactionLargePartitionWarnThresholdInMB() {
        return 100;
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
    public boolean isRestoreEncrypted() {
        return false;
    }

    @Override
	public String getAWSRoleAssumptionArn() {
		return null;
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

    @Override
    public String getFlushKeyspaces() {
        return "";
    }

    @Override
    public String getFlushInterval() {
        return null;
    }

    @Override
    public String getBackupStatusFileLoc() {
        return "backupstatus.ser";
    }

    @Override
    public boolean useSudo() {
        return true;
    }

    @Override
    public String getBackupNotificationTopicArn() {
        return null;
    }

    @Override
    public SchedulerType getFlushSchedulerType() throws UnsupportedTypeException {
        return SchedulerType.HOUR;
    }

    @Override
    public String getFlushCronExpression() {
        return null;
    }
}
