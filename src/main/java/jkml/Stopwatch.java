package jkml;

import java.time.Duration;

public class Stopwatch {

	private long startTimeMillis;

	public Stopwatch start() {
		startTimeMillis = System.currentTimeMillis();
		return this;
	}

	public Duration elapsed() {
		return Duration.ofMillis(System.currentTimeMillis() - startTimeMillis);
	}

}
