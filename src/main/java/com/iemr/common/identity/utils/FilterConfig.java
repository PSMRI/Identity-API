package com.iemr.common.identity.utils;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

@Configuration
public class FilterConfig {
	@Value("${cors.allowed-origins}")
	private String allowedOrigins;

	@Bean
	public FilterRegistrationBean<JwtUserIdValidationFilter> jwtUserIdValidationFilter(
			JwtAuthenticationUtil jwtAuthenticationUtil) {
		FilterRegistrationBean<JwtUserIdValidationFilter> registrationBean = new FilterRegistrationBean<>();

		JwtUserIdValidationFilter filter = new JwtUserIdValidationFilter(jwtAuthenticationUtil, allowedOrigins);
		registrationBean.setFilter(filter);
		registrationBean.setOrder(Ordered.HIGHEST_PRECEDENCE);
		registrationBean.addUrlPatterns("/*"); // Apply filter to all API endpoints
		return registrationBean;
	}

}
