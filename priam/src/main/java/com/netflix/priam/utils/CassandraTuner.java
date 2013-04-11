package com.netflix.priam.utils;

import java.io.IOException;

public interface CassandraTuner
{
    void updateYaml(String yamlLocation, String hostname, String seedProvider) throws IOException;

    void updateAutoBootstrap(String yamlLocation, boolean autobootstrap) throws IOException;
}
