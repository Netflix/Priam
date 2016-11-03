package com.netflix.priam.notification;

/**
 * Created by vinhn on 11/3/16.
 */
public class NoOpNotificationService implements INotificationService {
    @Override
    public void notifiy(String msg) {
        //NO OP
    }
}
