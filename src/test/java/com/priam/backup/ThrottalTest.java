package com.priam.backup;

import java.util.Random;

import org.junit.Test;

import com.priam.utils.Throttle;

public class ThrottalTest
{
    protected static final int THROTTAL_MB_PER_SEC = 1;

    @Test
    public void throttalTest()
    {
        Throttle throttle = new Throttle(this.getClass().getCanonicalName(), new Throttle.ThroughputFunction()
        {
            public int targetThroughput()
            {
                int totalBytesPerMS = (THROTTAL_MB_PER_SEC * 1024 * 1024)/1000;
                return totalBytesPerMS;
            }
        });
        long start = System.currentTimeMillis();
        Random ran = new Random();
        for (int i = 0; i < 10; i++)
        {
            long simulated = ran.nextInt(10) * 1024 * 1024;
            System.out.println("Simulating upload of "+ simulated  + " @ " + System.currentTimeMillis());
            throttle.throttle(simulated);
        }
        
        System.out.println("Completed in: " + (System.currentTimeMillis() - start));
    }
}
