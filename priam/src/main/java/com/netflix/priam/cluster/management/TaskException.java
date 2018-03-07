package com.netflix.priam.cluster.management;

/**
 * Created by aagrawal on 2/28/18.
 */
public class TaskException extends Exception{

    public TaskException(String message){
        super(message);
    }

    public TaskException(String message, Throwable e){
        super(message, e);
    }
}
