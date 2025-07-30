package com.iemr.common.identity.utils.exception;

public class TokenDenylistException extends RuntimeException {
	public TokenDenylistException(String message, Throwable cause) {
		super(message, cause);
	}
}
