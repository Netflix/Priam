package com.netflix.priam.scheduler;

import com.google.common.truth.Truth;
import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;
import org.quartz.Trigger;

public class TestSimpleTimer {
    private static final int PERIOD = 10;
    private static final Instant START = Instant.EPOCH.plus(5, ChronoUnit.SECONDS);

    @Test
    public void sunnyDay() throws ParseException {
        assertions(new SimpleTimer("foo", PERIOD, START).getTrigger(), START);
    }

    @Test
    public void startBeforeEpoch() {
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> new SimpleTimer("foo", PERIOD, Instant.EPOCH.minus(5, ChronoUnit.SECONDS)));
    }

    @Test
    public void startAtEpoch() throws ParseException {
        assertions(new SimpleTimer("foo", PERIOD, Instant.EPOCH).getTrigger(), Instant.EPOCH);
    }

    @Test
    public void startMoreThanOnePeriodAfterEpoch() throws ParseException {
        Instant start = Instant.EPOCH.plus(2 * PERIOD, ChronoUnit.SECONDS);
        assertions(new SimpleTimer("foo", PERIOD, start).getTrigger(), start);
    }

    @Test
    public void negativePeriod() {
        Assert.assertThrows(
                IllegalArgumentException.class, () -> new SimpleTimer("foo", -PERIOD, START));
    }

    @Test
    public void zeroPeriod() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new SimpleTimer("foo", 0, START));
    }

    private void assertions(Trigger trigger, Instant start) {
        Instant now = Instant.now();
        Instant nextFireTime = trigger.getFireTimeAfter(Date.from(now)).toInstant();
        Truth.assertThat(nextFireTime.getEpochSecond() % PERIOD)
                .isEqualTo(start.getEpochSecond() % PERIOD);
        Truth.assertThat(nextFireTime).isAtMost(Instant.now().plus(PERIOD, ChronoUnit.SECONDS));
        Truth.assertThat(trigger.getFinalFireTime()).isNull();
        Truth.assertThat(trigger.getEndTime()).isNull();
    }
}
