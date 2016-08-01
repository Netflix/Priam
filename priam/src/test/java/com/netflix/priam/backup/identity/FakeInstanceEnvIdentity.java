package com.netflix.priam.backup.identity;

import com.netflix.priam.identity.InstanceEnvIdentity;

public class FakeInstanceEnvIdentity implements InstanceEnvIdentity {

	@Override
	public Boolean isClassic() {
		return null;
	}

	@Override
	public Boolean isDefaultVpc() {
		return null;
	}

	@Override
	public Boolean isNonDefaultVpc() {
		return null;
	}

}