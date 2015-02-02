package com.netflix.priam.aws;

import java.util.List;

/*
 * An encapsulation of security rules.
 */
public interface IS3SecurityRuleMetadata {

	/*
	 * Fetch the ports (ssl and/or non-ssl) used for communication among nodes. 
	 */
	public List<Integer> getPorts();
}
