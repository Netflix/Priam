package com.netflix.priam.cluster.management;

import java.util.List;

/**
 * Created by vinhn on 10/12/16.
 */
public interface IClusterManagement<T> {

    public List<T> execute() throws Exception;
}
