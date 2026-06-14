package jkml;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public class ElapsedTimes {

	private final AtomicLong fetch = new AtomicLong();

	private final AtomicLong process = new AtomicLong();

	public ElapsedTimes addFetch(Duration value) {
		fetch.addAndGet(value.toMillis());
		return this;
	}

	public ElapsedTimes addProcess(Duration value) {
		process.addAndGet(value.toMillis());
		return this;
	}

	public Duration getFetch() {
		return Duration.ofMillis(fetch.get());
	}

	public Duration getProcess() {
		return Duration.ofMillis(process.get());
	}

}
