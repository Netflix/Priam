package com.netflix.priam.aws;

import java.util.*;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;

@Singleton
public class S3SecurityRuleMetadata implements IS3SecurityRuleMetadata {

	private IConfiguration config;

	@Inject
	public S3SecurityRuleMetadata(IConfiguration config) {
		this.config = config;
	}
	
	@Override
	public List<Integer> getPorts() {
		List<Integer> ports = new ArrayList<Integer>();
        ports.add(config.getSSLStoragePort()); //configure rule for ssl port
		return ports;
	}

}
