package com.netflix.priam;

import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.Map;

public final class SimpleDBConfigSource extends AbstractConfigSource {
  private static final Logger logger = LoggerFactory.getLogger(SimpleDBConfigSource.class.getName());

  private static final String DOMAIN = "PriamProperties";
  private static String ALL_QUERY = "select * from " + DOMAIN + " where " + Attributes.APP_ID + "='%s'";

  private final Map<String, String> data = Maps.newConcurrentMap();
  private final ICredential provider;

  @Inject
  public SimpleDBConfigSource(final ICredential provider) {
    this.provider = provider;
  }

  @Override
  public void intialize(final String asgName, final String region) {
    super.intialize(asgName, region);

    // End point is us-east-1
    AmazonSimpleDBClient simpleDBClient = new AmazonSimpleDBClient(provider.getCredentials());

    String nextToken = null;
    String appid = asgName.lastIndexOf('-') > 0 ? asgName.substring(0, asgName.indexOf('-')) : asgName;
    logger.info(String.format("appid used to fetch properties is: %s", appid));
    do {
      SelectRequest request = new SelectRequest(String.format(ALL_QUERY, appid));
      request.setNextToken(nextToken);
      SelectResult result = simpleDBClient.select(request);
      nextToken = result.getNextToken();
      Iterator<Item> itemiter = result.getItems().iterator();
      while (itemiter.hasNext())
        addProperty(itemiter.next());

    } while (nextToken != null);
  }

  private static class Attributes
  {
    public final static String APP_ID = "appId"; // ASG
    public final static String PROPERTY = "property";
    public final static String PROPERTY_VALUE = "value";
    public final static String REGION = "region";
  }

  private void addProperty(Item item)
  {
    Iterator<Attribute> attrs = item.getAttributes().iterator();
    String prop = "";
    String value = "";
    String dc = "";
    while (attrs.hasNext())
    {
      Attribute att = attrs.next();
      if (att.getName().equals(Attributes.PROPERTY))
        prop = att.getValue();
      else if (att.getName().equals(Attributes.PROPERTY_VALUE))
        value = att.getValue();
      else if (att.getName().equals(Attributes.REGION))
        dc = att.getValue();
    }
    // Ignore, if not this region
    if (StringUtils.isNotBlank(dc) && !dc.equals(getRegion()))
      return;
    // Override only if region is specified
    if (data.containsKey(prop) && StringUtils.isBlank(dc))
      return;
    data.put(prop, value);
  }

  @Override
  public int size() {
    return data.size();
  }

  @Override
  public String get(final String key) {
    return data.get(key);
  }

  @Override
  public void set(final String key, final String value) {
    data.put(key, value);
  }
}
