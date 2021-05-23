package com.netflix.priam.aws;

import com.google.common.base.Splitter;
import com.netflix.priam.identity.PriamInstance;
import com.netflix.priam.identity.config.InstanceInfo;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Derives public ips from PriamInstances with AWS hostnames. */
public class AWSIPConverter implements IPConverter {
    private static final Logger logger = LoggerFactory.getLogger(AWSIPConverter.class);
    private static final Pattern IP_PART = Pattern.compile("^[0-9]{1,3}$");

    private final InstanceInfo myInstance;

    @Inject
    public AWSIPConverter(InstanceInfo myInstance) {
        this.myInstance = myInstance;
    }

    @Override
    public Optional<String> getPublicIP(PriamInstance instance) {
        if (instance.getInstanceId().equals(myInstance.getInstanceId())) {
            return Optional.ofNullable(myInstance.getHostIP());
        }
        Optional<String> ip = extractIPFromHostnameString(instance.getHostName());
        return !instance.getDC().equals(myInstance.getRegion()) && !ip.isPresent()
                ? deriveIPFromHostname(instance.getHostName())
                : ip;
    }

    private Optional<String> extractIPFromHostnameString(String hostname) {
        if (hostname.contains(".")) {
            String ip = hostname.substring(4, hostname.indexOf('.')).replace('-', '.');
            List<String> parts = Splitter.on(".").splitToList(ip);
            return parts.size() == 4 && parts.stream().allMatch(p -> IP_PART.matcher(p).matches())
                    ? Optional.of(ip)
                    : Optional.empty();
        }
        return Optional.empty();
    }

    private Optional<String> deriveIPFromHostname(String hostname) {
        String ip = null;
        try {
            ip = InetAddress.getByName(hostname).getHostAddress();
            logger.info("Derived IP for " + hostname + ": " + ip);
        } catch (UnknownHostException e) {
            logger.warn("Could not resolve [" + hostname + "]");
        }
        return Optional.ofNullable(ip);
    }
}
