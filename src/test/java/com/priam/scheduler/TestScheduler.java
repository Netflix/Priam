package com.priam.scheduler;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.priam.TestModule;
import com.priam.conf.IConfiguration;

public class TestScheduler {
	private static final Logger logger = LoggerFactory
			.getLogger(TestScheduler.class);

	@Test
	public void testScedule() throws Exception {
		Injector inject = Guice.createInjector(new TestModule());
		PriamScheduler scheduler = inject.getInstance(PriamScheduler.class);
		scheduler.start();
		scheduler
				.addTask("test", VijayTest.class, new SimpleTimer("vijay", 10));
		scheduler.addTask("test1", VijayTest.class, new SimpleTimer("test1"));
		Thread.sleep(100);
		scheduler.shutdown();
	}

	public static class VijayTest extends Task {
		private IConfiguration config;

		@Inject
		public VijayTest(IConfiguration config) {
			this.config = config;
		}

		@Override
		public void execute() {
			logger.error(config.getAppName());
		}

		@Override
		public String getName() {
			return "test";
		}

	}
}
