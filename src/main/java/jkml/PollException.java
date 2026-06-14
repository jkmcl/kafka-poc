package jkml;

import java.io.Serial;

public class PollException extends RuntimeException {

	@Serial
	private static final long serialVersionUID = 1L;

	public PollException(String message) {
		super(message);
	}

}
