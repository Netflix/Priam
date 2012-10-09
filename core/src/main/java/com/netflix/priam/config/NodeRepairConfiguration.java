package com.netflix.priam.config;


import org.codehaus.jackson.annotate.JsonProperty;
import java.util.List;
import java.util.Map;


public class NodeRepairConfiguration {


    @JsonProperty
    private KeyspaceRepairConfiguration sorUgc;

    @JsonProperty
    private KeyspaceRepairConfiguration sorCat;

    @JsonProperty
    private KeyspaceRepairConfiguration databusCass;

    @JsonProperty
    private KeyspaceRepairConfiguration polloiCass;


    public KeyspaceRepairConfiguration getSorUgcConfiguration(){
        return sorUgc;
    }

    public KeyspaceRepairConfiguration getSorCatConfiguration(){
        return sorCat;
    }

    public KeyspaceRepairConfiguration getDatabusCassConfiguration(){
        return databusCass;
    }

    public KeyspaceRepairConfiguration getPolloiCassConfiguration(){
        return polloiCass;
    }
}
