package com.iemr.common.identity.utils.http;

import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;

public class AuthorizationHeaderRequestWrapper extends HttpServletRequestWrapper {
	private final String Authorization;

	public AuthorizationHeaderRequestWrapper(HttpServletRequest request, String authHeaderValue) {
		super(request);
		this.Authorization = authHeaderValue != null ? authHeaderValue : "";
	}

	@Override
	public String getHeader(String name) {
		if ("Authorization".equalsIgnoreCase(name)) {
			return Authorization;
		}
		return super.getHeader(name);
	}

	@Override
	public Enumeration<String> getHeaders(String name) {
		if ("Authorization".equalsIgnoreCase(name)) {
			return Authorization != null ? Collections.enumeration(Collections.singletonList(Authorization))
					: Collections.emptyEnumeration();
		}
		return super.getHeaders(name);
	}

	@Override
	public Enumeration<String> getHeaderNames() {
		List<String> names = Collections.list(super.getHeaderNames());
		boolean hasAuth = names.stream().anyMatch(name -> "Authorization".equalsIgnoreCase(name));
		if (!hasAuth) {
			names.add("Authorization");
		}
		return Collections.enumeration(names);
	}
}
