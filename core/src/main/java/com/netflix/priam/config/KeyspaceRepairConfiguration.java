package com.netflix.priam.config;

import com.google.common.collect.Maps;

import javax.validation.Valid;
import java.util.Map;

public class KeyspaceRepairConfiguration {
    @Valid
    private Map<String, String> us_east_1a = Maps.newHashMap();

    @Valid
    private Map<String, String> us_east_1b = Maps.newHashMap();

    @Valid
    private Map<String, String> us_east_1c = Maps.newHashMap();

    @Valid
    private Map<String, String> eu_west_1a = Maps.newHashMap();

    @Valid
    private Map<String, String> eu_west_1b = Maps.newHashMap();

    @Valid
    private Map<String, String> eu_west_1c = Maps.newHashMap();

    public Map<String, String> getUs_east_1a(){
        return us_east_1a;
    }

    public Map<String, String> getUs_east_1b(){
        return us_east_1b;
    }

    public Map<String, String> getUs_east_1c(){
        return us_east_1c;
    }

    public Map<String, String> getEu_west_1a(){
        return eu_west_1a;
    }

    public Map<String, String> getEu_west_1b(){
        return eu_west_1b;
    }

    public Map<String, String> getEu_west_1c(){
        return eu_west_1c;
    }
}
