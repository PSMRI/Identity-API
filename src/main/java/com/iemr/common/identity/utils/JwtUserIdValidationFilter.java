package com.iemr.common.identity.utils;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iemr.common.identity.utils.http.AuthorizationHeaderRequestWrapper;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class JwtUserIdValidationFilter implements Filter {
	private final JwtAuthenticationUtil jwtAuthenticationUtil;
	private final CookieUtil cookieUtil;
	private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	private final String allowedOrigins;

	public JwtUserIdValidationFilter(JwtAuthenticationUtil jwtAuthenticationUtil, String allowedOrigins, CookieUtil cookieUtil) {
		this.jwtAuthenticationUtil = jwtAuthenticationUtil;
		this.allowedOrigins = allowedOrigins;
		this.cookieUtil = cookieUtil;
	}

	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
			throws IOException, ServletException {
		HttpServletRequest request = (HttpServletRequest) servletRequest;
		HttpServletResponse response = (HttpServletResponse) servletResponse;

		handleCorsHeaders(request, response);
		
		if ("OPTIONS".equalsIgnoreCase(request.getMethod())) {
			logger.info("OPTIONS request - skipping JWT validation");
			response.setStatus(HttpServletResponse.SC_OK);
			return;
		}
		String path = request.getRequestURI();
		logger.info("JwtUserIdValidationFilter invoked for path: " + path);

		// Log cookies for debugging
		Cookie[] cookies = request.getCookies();
		if (cookies != null) {
			for (Cookie cookie : cookies) {
				if ("userId".equalsIgnoreCase(cookie.getName())) {
					logger.warn("userId found in cookies! Clearing it...");
					clearUserIdCookie(response); // Explicitly remove userId cookie
				}
			}
		} else {
			logger.info("No cookies found in the request");
		}

		// Log headers for debugging
		logger.info("JWT token from header: ");

		try {
			String jwtFromCookie = getJwtTokenFromCookies(request);
			String jwtFromHeader = request.getHeader("JwtToken");
			String authHeader = request.getHeader("Authorization");
			String jwtToken = jwtFromCookie != null ? jwtFromCookie : jwtFromHeader;
			if (jwtToken != null) {
				logger.info("Validating JWT token from cookie");
				if (jwtAuthenticationUtil.validateUserIdAndJwtToken(jwtToken)) {
					AuthorizationHeaderRequestWrapper authorizationHeaderRequestWrapper = new AuthorizationHeaderRequestWrapper(
							request, "");
					filterChain.doFilter(authorizationHeaderRequestWrapper, servletResponse);
					return;
				}
			} else {
				String userAgent = request.getHeader("User-Agent");
				logger.info("User-Agent: " + userAgent);
				if (userAgent != null && isMobileClient(userAgent) && authHeader != null) {
					try {
						logger.info("Common-API incoming userAget : " + userAgent);
						UserAgentContext.setUserAgent(userAgent);
						filterChain.doFilter(servletRequest, servletResponse);
					} finally {
						UserAgentContext.clear();
					}
					return;
				}
			}

			logger.warn("No valid authentication token found");
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Unauthorized: Invalid or missing token");
		} catch (Exception e) {
			logger.error("Authorization error: ", e);
			response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Authorization error: " + e.getMessage());
		}
	}

	private void handleCorsHeaders(HttpServletRequest request, HttpServletResponse response) {
		String origin = request.getHeader("Origin");

		logger.debug("Incoming Origin: {}", origin);
		logger.debug("Allowed Origins Configured: {}", allowedOrigins);

		if (origin != null && isOriginAllowed(origin)) {
			response.setHeader("Access-Control-Allow-Origin", origin);
			response.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
			response.setHeader("Access-Control-Allow-Headers",
					"Authorization, Content-Type, Accept, JwtToken, Jwttoken");
			response.setHeader("Vary", "Origin");
			response.setHeader("Access-Control-Allow-Credentials", "true");
		} else {
			logger.warn("Origin [{}] is NOT allowed. CORS headers NOT added.", origin);
		}
		
	}

	private boolean isOriginAllowed(String origin) {
		if (origin == null || allowedOrigins == null || allowedOrigins.trim().isEmpty()) {
			logger.warn("No allowed origins configured or origin is null");
			return false;
		}

		return Arrays.stream(allowedOrigins.split(",")).map(String::trim).anyMatch(pattern -> {
			String regex = pattern.replace(".", "\\.").replace("*", ".*").replace("http://localhost:.*",
					"http://localhost:\\d+"); // special case for wildcard port

			boolean matched = origin.matches(regex);
			return matched;
		});
	}

	private boolean isMobileClient(String userAgent) {
		if (userAgent == null)
			return false;
		userAgent = userAgent.toLowerCase();
		return userAgent.contains("okhttp") || userAgent.contains("java/"); // iOS (custom clients)
	}

	private String getJwtTokenFromCookies(HttpServletRequest request) {
		return cookieUtil.getJwtTokenFromCookie(request);
	}

	private void clearUserIdCookie(HttpServletResponse response) {
		Cookie cookie = new Cookie("userId", null);
		cookie.setPath("/");
		cookie.setHttpOnly(true);
		cookie.setSecure(true);
		cookie.setMaxAge(0); // Invalidate the cookie
		cookie.setAttribute("SameSite", "Strict");
		response.addCookie(cookie);
	}
}
