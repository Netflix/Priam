package com.netflix.priam.utils;

import com.google.inject.ImplementedBy;
import com.netflix.priam.defaultimpl.StandardTuner;

import java.io.IOException;

@ImplementedBy(StandardTuner.class)
public interface CassandraTuner
{
    void updateYaml(String yamlLocation, String hostname, String seedProvider) throws IOException;

    void updateAutoBootstrap(String yamlLocation, boolean autobootstrap) throws IOException;
}
