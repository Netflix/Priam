package com.netflix.priam.identity.config;

import org.codehaus.jettison.json.JSONException;

import com.netflix.priam.utils.SystemUtils;

/**
 * Calls AWS metadata to get info on the location of the running instance within classic environment.
 *
 */
public class AwsClassicInstanceDataRetriever extends InstanceDataRetrieverBase implements InstanceDataRetriever {

    public String getRac()
    {
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/placement/availability-zone");
    }

    public String getPublicHostname()
    {
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-hostname");
    }

    public String getPublicIP()
    {
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/public-ipv4");
    }

    public String getInstanceId()
    {
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/instance-id");
    }

    public String getInstanceType()
    {
        return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/instance-type");
    }

	@Override
	/*
	 * @return id of the network interface for running instance
	 */
	public String getMac() {
		return SystemUtils.getDataFromUrl("http://169.254.169.254/latest/meta-data/network/interfaces/macs/").trim();
	}

	@Override
	public String getVpcId() {
		throw new UnsupportedOperationException("Not applicable as running instance is in classic environment");
	}

}