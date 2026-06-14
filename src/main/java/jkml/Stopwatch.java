package jkml;

import java.time.Duration;

public class Stopwatch {

	private long start;

	public Stopwatch start() {
		start = System.currentTimeMillis();
		return this;
	}

	public Duration elapsed() {
		return Duration.ofMillis(System.currentTimeMillis() - start);
	}

}
