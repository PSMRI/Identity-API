package com.iemr.common.identity.utils.exception;

public class TokenDenylistException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public TokenDenylistException(String message) {
		super(message);
	}

	public TokenDenylistException(String message, Throwable cause) {
		super(message, cause);
	}

	public TokenDenylistException(Throwable cause) {
		super(cause);
	}
}
