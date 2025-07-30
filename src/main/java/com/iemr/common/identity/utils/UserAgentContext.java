package com.iemr.common.identity.utils;

public class UserAgentContext {
	private static final ThreadLocal<String> userAgentHolder = new ThreadLocal<>();

	public static void setUserAgent(String userAgent) {
		if (userAgent != null && userAgent.trim().isEmpty()) {
			userAgent = null; // Treat empty strings as null
		}
		userAgentHolder.set(userAgent);
	}

    public static String getUserAgent() {
        return userAgentHolder.get();
    }

    public static void clear() {
        userAgentHolder.remove();
    }
}
