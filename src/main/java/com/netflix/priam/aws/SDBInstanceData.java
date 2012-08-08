package com.netflix.priam.aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.amazonaws.services.simpledb.model.UpdateCondition;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.ICredential;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.identity.PriamInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * DAO for handling Instance identity information such as token, zone, region
 */
@Singleton
public class SDBInstanceData
{
    public static class Attributes
    {
        public final static String APP_ID = "appId";
        public final static String ID = "id";
        public final static String INSTANCE_ID = "instanceId";
        public final static String TOKEN = "token";
        public final static String AVAILABILITY_ZONE = "availabilityZone";
        public final static String ELASTIC_IP = "elasticIP";
        public final static String UPDATE_TS = "updateTimestamp";
        public final static String LOCATION = "location";
        public final static String HOSTNAME = "hostname";
    }

    private static final Logger logger = LoggerFactory.getLogger(SDBInstanceData.class);

    private final ICredential provider;
    private final AmazonConfiguration amazonConfiguration;
    
    @Inject
    public SDBInstanceData(ICredential provider, AmazonConfiguration amazonConfiguration)
    {
        this.provider = provider;
        this.amazonConfiguration = amazonConfiguration;

        createDomain();  // This is idempotent and won't affect the domain if it already exists
    }

    private String getAllQuery()
    {
        return "select * from " + amazonConfiguration.getSimpleDbDomain() + " where " + Attributes.APP_ID + "='%s'";
    }

    private String getInstanceQuery()
    {
        return "select * from " + amazonConfiguration.getSimpleDbDomain() + " where " + Attributes.APP_ID + "='%s' and " + Attributes.ID + "='%d'";
    }

    private void createDomain()
    {
        logger.info("Creating SimpleDB domain '{}'", amazonConfiguration.getSimpleDbDomain());
        AmazonSimpleDBClient simpleDBClient = getSimpleDBClient();
        CreateDomainRequest request = new CreateDomainRequest(amazonConfiguration.getSimpleDbDomain());
        simpleDBClient.createDomain(request);
    }

    /**
     * Get the instance details from SimpleDB
     * 
     * @param app Cluster name
     * @param id Node ID
     * @return the node with the given {@code id}, or {@code null} if no such node exists
     */
    public PriamInstance getInstance(String app, int id)
    {
        AmazonSimpleDBClient simpleDBClient = getSimpleDBClient();
        SelectRequest request = new SelectRequest(String.format(getInstanceQuery(), app, id));
        SelectResult result = simpleDBClient.select(request);
        if (result.getItems().size() == 0)
            return null;
        PriamInstance priamInstance = transform(result.getItems().get(0));
        logger.info("Retrieved instance from SimpleDB: {}", priamInstance);
        return priamInstance;
    }

    /**
     * Get the set of all nodes in the cluster
     * 
     * @param app Cluster name
     * @return the set of all instances in the given {@code app}
     */
    public Set<PriamInstance> getAllIds(String app)
    {
        AmazonSimpleDBClient simpleDBClient = getSimpleDBClient();
        Set<PriamInstance> inslist = new HashSet<PriamInstance>();
        String nextToken = null;
        String allQuery = getAllQuery();
        do
        {
            SelectRequest request = new SelectRequest(String.format(allQuery, app));
            request.setNextToken(nextToken);
            SelectResult result = simpleDBClient.select(request);
            nextToken = result.getNextToken();
            Iterator<Item> itemiter = result.getItems().iterator();
            while (itemiter.hasNext())
            {
                PriamInstance priamInstance = transform(itemiter.next());
                logger.info("Retrieved PriamInstance from app '{}': {}", app, priamInstance);
                inslist.add(priamInstance);
            }

        } while (nextToken != null);
        return inslist;
    }

    /**
     * Create a new instance entry in SimpleDB
     * 
     * @param instance
     * @throws AmazonServiceException
     */
    public void createInstance(PriamInstance instance) throws AmazonServiceException
    {
        logger.info("Creating PriamInstance in SimpleDB: {}", instance);
        AmazonSimpleDBClient simpleDBClient = getSimpleDBClient();
        PutAttributesRequest putReq = new PutAttributesRequest(amazonConfiguration.getSimpleDbDomain(), getKey(instance), createAttributesToRegister(instance));
        simpleDBClient.putAttributes(putReq);
    }

    /**
     * Register a new instance. Registration will fail if a prior entry exists
     * 
     * @param instance
     * @throws AmazonServiceException
     */
    public void registerInstance(PriamInstance instance) throws AmazonServiceException
    {
        logger.info("Registering PriamInstance in SimpleDB: {}", instance);
        AmazonSimpleDBClient simpleDBClient = getSimpleDBClient();
        PutAttributesRequest putReq = new PutAttributesRequest(amazonConfiguration.getSimpleDbDomain(), getKey(instance), createAttributesToRegister(instance));
        UpdateCondition expected = new UpdateCondition();
        expected.setName(Attributes.INSTANCE_ID);
        expected.setExists(false);
        putReq.setExpected(expected);
        simpleDBClient.putAttributes(putReq);
    }

    /**
     * Deregister instance (same as delete)
     * 
     * @param instance
     * @throws AmazonServiceException
     */
    public void deregisterInstance(PriamInstance instance) throws AmazonServiceException
    {
        logger.info("De-Registering PriamInstance from SimpleDB: {}", instance);
        AmazonSimpleDBClient simpleDBClient = getSimpleDBClient();
        DeleteAttributesRequest delReq = new DeleteAttributesRequest(amazonConfiguration.getSimpleDbDomain(), getKey(instance), createAttributesToDeRegister(instance));
        simpleDBClient.deleteAttributes(delReq);
    }

    private List<ReplaceableAttribute> createAttributesToRegister(PriamInstance instance)
    {
        instance.setUpdatetime(new Date().getTime());
        List<ReplaceableAttribute> attrs = new ArrayList<ReplaceableAttribute>();
        attrs.add(new ReplaceableAttribute(Attributes.INSTANCE_ID, instance.getInstanceId(), false));
        attrs.add(new ReplaceableAttribute(Attributes.TOKEN, instance.getToken(), true));
        attrs.add(new ReplaceableAttribute(Attributes.APP_ID, instance.getApp(), true));
        attrs.add(new ReplaceableAttribute(Attributes.ID, Integer.toString(instance.getId()), true));
        attrs.add(new ReplaceableAttribute(Attributes.AVAILABILITY_ZONE, instance.getRac(), true));
        attrs.add(new ReplaceableAttribute(Attributes.ELASTIC_IP, instance.getHostIP(), true));
        attrs.add(new ReplaceableAttribute(Attributes.HOSTNAME, instance.getHostName(), true));
        attrs.add(new ReplaceableAttribute(Attributes.LOCATION, instance.getDC(), true));
        attrs.add(new ReplaceableAttribute(Attributes.UPDATE_TS, Long.toString(instance.getUpdatetime()), true));
        return attrs;
    }

    private List<Attribute> createAttributesToDeRegister(PriamInstance instance)
    {
        List<Attribute> attrs = new ArrayList<Attribute>();
        attrs.add(new Attribute(Attributes.INSTANCE_ID, instance.getInstanceId()));
        attrs.add(new Attribute(Attributes.TOKEN, instance.getToken()));
        attrs.add(new Attribute(Attributes.APP_ID, instance.getApp()));
        attrs.add(new Attribute(Attributes.ID, Integer.toString(instance.getId())));
        attrs.add(new Attribute(Attributes.AVAILABILITY_ZONE, instance.getRac()));
        attrs.add(new Attribute(Attributes.ELASTIC_IP, instance.getHostIP()));
        attrs.add(new Attribute(Attributes.HOSTNAME, instance.getHostName()));
        attrs.add(new Attribute(Attributes.LOCATION, instance.getDC()));
        attrs.add(new Attribute(Attributes.UPDATE_TS, Long.toString(instance.getUpdatetime())));
        return attrs;
    }

    /**
     * Convert a simpledb item to PriamInstance
     * 
     * @param item
     * @return
     */
    private PriamInstance transform(Item item)
    {
        PriamInstance ins = new PriamInstance();
        Iterator<Attribute> attrs = item.getAttributes().iterator();
        while (attrs.hasNext())
        {
            Attribute att = attrs.next();
            if (att.getName().equals(Attributes.INSTANCE_ID))
                ins.setInstanceId(att.getValue());
            else if (att.getName().equals(Attributes.TOKEN))
                ins.setToken(att.getValue());
            else if (att.getName().equals(Attributes.APP_ID))
                ins.setApp(att.getValue());
            else if (att.getName().equals(Attributes.ID))
                ins.setId(Integer.parseInt(att.getValue()));
            else if (att.getName().equals(Attributes.AVAILABILITY_ZONE))
                ins.setRac(att.getValue());
            else if (att.getName().equals(Attributes.ELASTIC_IP))
                ins.setHostIP(att.getValue());
            else if (att.getName().equals(Attributes.HOSTNAME))
                ins.setHost(att.getValue());
            else if (att.getName().equals(Attributes.LOCATION))
                ins.setDC(att.getValue());
            else if (att.getName().equals(Attributes.UPDATE_TS))
                ins.setUpdatetime(Long.parseLong(att.getValue()));
        }
        return ins;
    }

    private String getKey(PriamInstance instance)
    {
        return instance.getApp() + instance.getId();
    }
    
    private AmazonSimpleDBClient getSimpleDBClient(){
        //Create per request
        return new AmazonSimpleDBClient(provider.getCredentials());
    }
}
