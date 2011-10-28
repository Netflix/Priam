package com.priam.netflix;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.TimeUUIDUtils;
import com.netflix.cassandra.KeyspaceFactory;
import com.netflix.cassandra.NFAstyanaxManager;
import com.netflix.instance.identity.InstanceData;
import com.netflix.instance.identity.StorageDevice;
import com.netflix.instance.identity.aws.InstanceDataDAOSimpleDb;
import com.netflix.library.NFLibraryManager;
import com.netflix.platform.core.PlatformManager;
import com.priam.identity.PriamInstance;
import com.priam.utils.SystemUtils;
import com.priam.utils.TokenManager;

/**
 * Utility to do the following: - Migrate tokens from simpledb to cassandra -
 * List tokens in cassandra - Delete tokens in cassandra
 * 
 * @author Praveen Sadhu
 * 
 */
public class TokenMigrationUtil
{
    private static final Logger logger = LoggerFactory.getLogger(InstanceDataDAOCassandra.class);
    private Keyspace bootKeyspace;
    public static final ColumnFamily<String, String> CF_TOKENS = new ColumnFamily<String, String>("tokens", StringSerializer.get(), StringSerializer.get());
    public static final ColumnFamily<String, String> CF_LOCKS = new ColumnFamily<String, String>("locks", StringSerializer.get(), StringSerializer.get());
    public static String BOOT_CLUSTER_NAME = "cass_turtle";
    private static final String CN_ID = "Id";
    private static final String CN_APPID = "appId";
    private static final String CN_AZ = "availabilityZone";
    private static final String CN_INSTANCEID = "instanceId";
    private static final String CN_HOSTNAME = "hostname";
    private static final String CN_EIP = "elasticIP";
    private static final String CN_TOKEN = "token";
    private static final String CN_LOCATION = "location";
    private static final String CN_VOLUME_PREFIX = "ssVolumes";
    private static final String CN_UPDATETIME = "updatetime";

    public TokenMigrationUtil()
    {
        Properties props = new Properties();
        props.setProperty(BOOT_CLUSTER_NAME + ".bootstrap.astyanax.readConsistency", "CL_QUORUM");
        props.setProperty(BOOT_CLUSTER_NAME + ".bootstrap.astyanax.writeConsistency", "CL_QUORUM");
        props.setProperty("platform.ListOfComponentsToInit", "LOGGING,APPINFO,DISCOVERY,AWS");
        props.setProperty("platform.ListOfMandatoryComponentsToInit", "");
        props.setProperty("netflix.environment", System.getenv("NETFLIX_ENVIRONMENT"));
        System.setProperty("netflix.logging.realtimetracers", "true");
        System.setProperty("netflix.appinfo.name", "nfcassandra.unittest");

        try
        {
            NFLibraryManager.initLibrary(PlatformManager.class, props, true, false);
            NFLibraryManager.initLibrary(NFAstyanaxManager.class, props, true, false);
            bootKeyspace = KeyspaceFactory.openKeyspace(BOOT_CLUSTER_NAME, "cassbootstrap");
        }
        catch (Exception e)
        {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void listTokens(String appName)
    {
        Map<String, PriamInstance> map = getAllInstances(appName);
        System.out.println(String.format("KEY | REGION | ZONE | INSTANCE ID | TOKEN "));
        // Pretty print
        for (String key : map.keySet())
        {
            PriamInstance ins = map.get(key);
            System.out.println(String.format("%s | %s | %s | %s | %s ", key, ins.location, ins.availabilityZone, ins.instanceId, ins.payload));
        }
    }

    public void deleteAllTokens(String appName) throws Exception
    {
        System.out.println("Deleting entries for " + appName);
        Map<String, PriamInstance> map = getAllInstances(appName);
        for (String key : map.keySet())
        {
            System.out.println("Deleting " + key);
            deleteInstanceEntry(map.get(key));
        }
    }

    public void deleteKey(String key) throws Exception
    {
        // Delete the row
        MutationBatch m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_TOKENS, key).delete();
        m.execute();

        m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_LOCKS, key + "-lock").delete();
        m.execute();

        m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_LOCKS, key + "-choosing").delete();
        m.execute();
    }

    public void deleteInstanceEntry(PriamInstance instance) throws Exception
    {
        // Delete the row
        String key = getRowKey(instance);
        MutationBatch m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_TOKENS, key).delete();
        m.execute();

        key = getLockingKey(instance);
        // Delete key
        m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_LOCKS, key).delete();
        m.execute();

        // Have to delete choosing key as well to avoid issues with delete
        // followed by immediate writes
        key = getChoosingKey(instance);
        m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_LOCKS, key).delete();
        m.execute();
    }

    public void migrate(String appName) throws Exception
    {
        for (InstanceData data : InstanceDataDAOSimpleDb.getInstance().getAllIds(appName))
        {
            PriamInstance ins = transform(data);
            System.out.println("Migrating: " + ins.getInstanceId() + " " + ins.getPayload());
            createInstanceEntry(ins);
        }
        System.out.println("Done migrating " + appName);
    }

    public Map<String, PriamInstance> getAllInstances(String app)
    {
        Map<String, PriamInstance> map = new HashMap<String, PriamInstance>();
        try
        {
            bootKeyspace.prepareQuery(CF_TOKENS).searchWithIndex();
            OperationResult<Rows<String, String>> result;
            result = bootKeyspace.prepareQuery(CF_TOKENS).searchWithIndex().setStartKey("").addExpression().whereColumn(CN_APPID).equals().value(app).execute();

            for (Row<String, String> row : result.getResult())
            {
                map.put(row.getKey(), transform(row.getColumns()));
            }
        }
        catch (ConnectionException e)
        {
            throw new RuntimeException(e);
        }
        return map;
    }

    public void createInstanceEntry(PriamInstance instance) throws Exception
    {
        String key = getRowKey(instance);
        // If the key exists throw exception
        if (getInstance(instance.getApp(), instance.getId()) != null)
            throw new Exception(String.format("Key already exists: %s", key));
        MutationBatch m = bootKeyspace.prepareMutationBatch();
        ColumnListMutation<String> clm = m.withRow(CF_TOKENS, key);
        clm.putColumn(CN_ID, Integer.toString(instance.getId()), null);
        clm.putColumn(CN_APPID, instance.getApp(), null);
        clm.putColumn(CN_AZ, instance.getRac(), null);
        clm.putColumn(CN_INSTANCEID, instance.getInstanceId(), null);
        clm.putColumn(CN_HOSTNAME, instance.getHostName(), null);
        clm.putColumn(CN_EIP, instance.getHostIP(), null);
        clm.putColumn(CN_TOKEN, instance.getPayload(), null);
        clm.putColumn(CN_LOCATION, instance.getDC(), null);
        clm.putColumn(CN_UPDATETIME, TimeUUIDUtils.getUniqueTimeUUIDinMicros(), null);
        Map<String, StorageDevice> volumes = instance.getVolumes();
        if (volumes != null)
        {
            for (String path : volumes.keySet())
            {
                clm.putColumn(CN_VOLUME_PREFIX + "_" + path, volumes.get(path).toString(), null);
            }
        }
        m.execute();
    }

    public PriamInstance getInstance(String app, int id)
    {
        Map<String, PriamInstance> map = getAllInstances(app);
        for (String key : map.keySet())
        {
            if (map.get(key).getId() == id)
                return map.get(key);

        }
        return null;
    }

    private PriamInstance transform(ColumnList<String> columns)
    {
        PriamInstance ins = new PriamInstance();
        Map<String, String> cmap = new HashMap<String, String>();
        for (Column<String> column : columns)
        {
            cmap.put(column.getName(), column.getStringValue());
        }
        ins.setApp(cmap.get(CN_APPID));
        ins.setRac(cmap.get(CN_AZ));
        ins.setHost(cmap.get(CN_HOSTNAME));
        ins.setHostIP(cmap.get(CN_EIP));
        ins.setId(Integer.parseInt(cmap.get(CN_ID)));
        ins.setInstanceId(cmap.get(CN_INSTANCEID));
        ins.setDC(cmap.get(CN_LOCATION));
        ins.setPayload(cmap.get(CN_TOKEN));
        return ins;
    }

    private PriamInstance transform(InstanceData data)
    {
        PriamInstance ins = new PriamInstance();
        ins.setApp(data.getApp());
        ins.setRac(data.getAvailabilityZone());
        String[] split = data.getElasticIP().split(",");
        ins.setHost(split[0], split[1]);
        ins.setId(data.getId());
        ins.setInstanceId(data.getInstanceId());
        ins.setDC(data.getLocation());
        ins.setPayload(data.getPayload());
        ins.setVolumes(data.getVolumes());
        return ins;
    }

    private String getChoosingKey(PriamInstance instance)
    {
        return instance.getApp() + instance.getId() + "-choosing";
    }

    private String getLockingKey(PriamInstance instance)
    {
        return instance.getApp() + instance.getId() + "-lock";
    }

    private String getRowKey(PriamInstance instance)
    {
        return instance.getApp() + instance.getId();
    }

    public void rebuild(String appName, String region, boolean useOldFormat) throws Exception
    {
        // Find all ASGs
        // For each asg, get all instances
        // Make a list of tokens and instances (collection: zone, instanceid,
        // token)
        String eiphost = getEPHost(region, System.getenv("NETFLIX_ENVIRONMENT"));
        String jsonText = stringOfUrl("http://" + eiphost + ":7101/REST/v1/asg", true);
        JSONParser parser = new JSONParser();

        try
        {
            JSONObject jsonObject = (JSONObject) parser.parse(jsonText);
            JSONObject asgs = (JSONObject) jsonObject.get("asgs");
            JSONArray asgarr = (JSONArray) asgs.get("asg");
            Iterator asgiter = asgarr.iterator();
            int asgCount = 0;
            List<PriamInstance> plist = new ArrayList<PriamInstance>();
            Map<String, Integer> slotMap = new HashMap<String, Integer>();
            while (asgiter.hasNext())
            {
                JSONObject asg = (JSONObject) asgiter.next();
                if (!((String) asg.get("name")).startsWith(appName + "-"))
                    continue;
                ++asgCount;

                logger.info("Iterating instances for " + (String) asg.get("name"));
                String zone = "";
                Iterator insiter = ((JSONArray) ((JSONObject) asg.get("instances")).get("instance")).iterator();
                while (insiter.hasNext())
                {
                    PriamInstance ins = buildFromJson((JSONObject) insiter.next(), appName, region);
                    zone = ins.getRac();
                    plist.add(ins);
                }
                slotMap.put(zone, new Integer(asgCount - 1));
            }
            Collections.sort(plist, new Comparator<PriamInstance>()
            {
                @Override
                public int compare(PriamInstance arg0, PriamInstance arg1)
                {
                    BigInteger b0 = new BigInteger(arg0.getPayload());
                    BigInteger b1 = new BigInteger(arg1.getPayload());
                    return b0.compareTo(b1);
                }
            });

            List<PriamInstance> emptyIns = new ArrayList<PriamInstance>();
            int hash = SystemUtils.hash(region);
            // Find the right slot. If slot is missing, add empty one
            for (PriamInstance ins : plist)
            {
                int myslot = Integer.valueOf(slotMap.get(ins.getRac()));
                BigInteger token = new BigInteger(TokenManager.createToken(myslot, plist.size(), region));
                BigInteger ntoken = new BigInteger(ins.getPayload());
                while (token.compareTo(ntoken) < 0 && !useOldFormat)
                {                    
                    slotMap.put(ins.getRac(), myslot + asgCount);
                    myslot = Integer.valueOf(slotMap.get(ins.getRac()));
                    token = new BigInteger(TokenManager.createToken(myslot, plist.size(), region));
                    logger.info("Missing token. Inserting empty instance token" + token);
                    emptyIns.add(getEmptyInstance(appName, ins.getRac(), region, hash + myslot, token.toString()));
                }
                ins.setId(hash + myslot);
                logger.info(String.format("%s %s %s slot = %d", ins.getInstanceId(), ins.getRac(), ins.getPayload(), ins.getId()));
                createInstanceEntry(ins);
                slotMap.put(ins.getRac(), myslot + asgCount);
            }
            for (PriamInstance ins : emptyIns)
                createInstanceEntry(ins);

        }
        catch (ParseException pe)
        {
            logger.error("Exception in json parsing");
            throw pe;
        }
    }

    public PriamInstance getEmptyInstance(String appName, String zone, String region, int id, String token)
    {
        PriamInstance ins = new PriamInstance();
        ins.setApp(appName);
        ins.setRac(zone);
        ins.setHost("new_host");
        ins.setHostIP("new_EIP");
        ins.setInstanceId("new_ins");
        ins.setDC(region);
        ins.setPayload(token);
        ins.setId(id);
        return ins;
    }

    public PriamInstance buildFromJson(JSONObject jsInstance, String appName, String region) throws Exception
    {

        // Get instances in the asg
        PriamInstance ins = new PriamInstance();
        ins.setApp(appName);
        ins.setRac((String) jsInstance.get("zone"));
        ins.setHost((String) jsInstance.get("publicDnsName"));
        ins.setHostIP((String) jsInstance.get("publicIp"));
        ins.setInstanceId((String) jsInstance.get("instanceId"));
        ins.setDC(region);
        logger.info("Fetching token for " + ins.getInstanceId());
        String token = stringOfUrl("http://" + ins.getHostName() + ":8080/Priam/REST/cassandra_config/test?type=GET_TOKEN", false);
        ins.setPayload(token);
        logger.info(String.format("Token for %s is %s", ins.getInstanceId(), ins.getPayload()));
        return ins;
    }

    public static String getEPHost(String region, String env) throws Exception
    {
        String disHost = "";
        if (region.equals("us-east-1"))
        {
            if (env.equalsIgnoreCase("prod"))
                disHost = "discovery.cloud.netflix.net";
            else
                disHost = "discovery.cloudqa.netflix.net";
        }
        else if (region.equals("eu-west-1"))
        {
            if (env.equalsIgnoreCase("prod"))
                disHost = "eu-west-1a.discovery.cloud.netflix.net";
            else
                disHost = "eu-west-1a.discovery.cloudqa.netflix.net";
        }
        else if (region.equals("us-west-1"))
        {
            if (env.equalsIgnoreCase("prod"))
                disHost = "us-west-1a.discovery.cloud.netflix.net";
            else
                disHost = "us-west-1a.discovery.cloudqa.netflix.net";
        }
        
        String eipuri = String.format("http://%s:7101/discovery/v1/apps/entrypoints", disHost);
        String jsonText = stringOfUrl(eipuri, true);
        JSONParser parser = new JSONParser();
        try
        {
            JSONObject jsonObject = (JSONObject) parser.parse(jsonText);
            JSONObject app = (JSONObject) jsonObject.get("application");
            JSONArray instances = (JSONArray) app.get("instance");
            return (String) ((JSONObject) instances.get(0)).get("hostName");
        }
        catch (ParseException pe)
        {
            logger.error("Exception in parsing entry point host from discovery");
            throw pe;
        }
    }

    public static String stringOfUrl(String addr, boolean jsonheader) throws Exception
    {
        StringBuffer retval = new StringBuffer("");
        URL url = new URL(addr);
        HttpURLConnection uc = (HttpURLConnection) url.openConnection();
        if (jsonheader)
            uc.addRequestProperty("Accept", "application/json");
        int responsecode = uc.getResponseCode();
        logger.info(String.format("%s response code=%d", addr, responsecode));
        if (responsecode != 200)
            throw new Exception("Cannot connect to " + addr);
        BufferedReader in = new BufferedReader(new InputStreamReader(uc.getInputStream()));
        String inputline;
        while ((inputline = in.readLine()) != null)
        {
            retval.append(inputline);
        }
        uc.disconnect();
        return retval.toString();
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        Options options = new Options();
        options.addOption("h", "help", false, "Help");
        options.addOption("A", "app_name", true, "App name");
        options.addOption("l", "list", false, "List tokens in cassandra");
        options.addOption("D", "delete_all", false, "Delete all token entries for the specified app");
        options.addOption("d", "delete_key", false, "Delete the specified row key (-k)");
        options.addOption("r", "region", true, "Region");
        options.addOption("K", "row_key", true, "Row key");
        options.addOption("M", "migrate", false, "Migrate the specified app from simpledb");
        options.addOption("R", "rebuild", false, "Rebuild entries from instances");
        options.addOption("o", "old_format", false, "FOr rebuilding, use the old format of token assignment");
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (args.length == 0 || cmd.hasOption("help"))
            TokenMigrationUtil.showUsageAndExit("", "", options);

        // delete specific key
        if (cmd.hasOption("d") && cmd.hasOption("K"))
        {
            System.out.println("Deleting key " + cmd.getOptionValue("K"));
            new TokenMigrationUtil().deleteKey(cmd.getOptionValue("K"));
            return;
        }
        else if (cmd.hasOption("d"))
            TokenMigrationUtil.showUsageAndExit("Invalid command line args", "Missing row key value", options);

        // List all keys
        if (cmd.hasOption("l") && cmd.hasOption("A"))
        {
            new TokenMigrationUtil().listTokens(cmd.getOptionValue("A"));
            return;
        }
        else if (cmd.hasOption("l"))
            TokenMigrationUtil.showUsageAndExit("Invalid command line args", "Missing app name", options);

        // Delete all entries
        if (cmd.hasOption("D") && cmd.hasOption("A"))
        {
            new TokenMigrationUtil().deleteAllTokens(cmd.getOptionValue("A"));
            return;
        }
        else if (cmd.hasOption("D"))
            TokenMigrationUtil.showUsageAndExit("Invalid command line args", "Missing app name", options);

        // Rebuild from the app instances
        if (cmd.hasOption("R") && cmd.hasOption("A"))
        {
            new TokenMigrationUtil().rebuild(cmd.getOptionValue("A"), cmd.getOptionValue("r"), cmd.hasOption("o"));
            return;
        }
        else if (cmd.hasOption("R"))
            TokenMigrationUtil.showUsageAndExit("Invalid command line args", "Missing app name", options);

        // Migrate from simpledb
        if (cmd.hasOption("M") && cmd.hasOption("A"))
        {
            new TokenMigrationUtil().migrate(cmd.getOptionValue("A"));
            return;
        }
        else if (cmd.hasOption("M"))
            TokenMigrationUtil.showUsageAndExit("Invalid command line args", "Missing app name", options);

    }

    public static void showUsageAndExit(String header, String footer, Options options)
    {
        HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp("TokenUtils", header, options, footer);
        System.exit(1);
    }
}
