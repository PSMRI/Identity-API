package com.iemr.common.identity.utils;

import java.util.Arrays;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Service
public class CookieUtil {
	
	public Optional<String> getCookieValue(HttpServletRequest request, String cookieName) {
		Cookie[] cookies = request.getCookies();
		if (cookies != null) {
			for (Cookie cookie : cookies) {
				if (cookieName.equals(cookie.getName())) {
					return Optional.of(cookie.getValue());
				}
			}
		}
		return Optional.empty();
	}

	public void addJwtTokenToCookie(String Jwttoken, HttpServletResponse response, HttpServletRequest request) {
		Cookie cookie = new Cookie("Jwttoken", Jwttoken);
		cookie.setHttpOnly(true);
		cookie.setMaxAge(60 * 60 * 24);
		cookie.setPath("/");
		if ("https".equalsIgnoreCase(request.getScheme())) {
			cookie.setSecure(true);
		}
		response.addCookie(cookie);
	}

	public String getJwtTokenFromCookie(HttpServletRequest request) {
		return getCookieValue(request, "Jwttoken").orElse(null);
	}
}
