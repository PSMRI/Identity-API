/*
* AMRIT – Accessible Medical Records via Integrated Technology
* Integrated EHR (Electronic Health Records) Solution
*
* Copyright (C) "Piramal Swasthya Management and Research Institute"
*
* This file is part of AMRIT.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program.  If not, see https://www.gnu.org/licenses/.
*/
package com.iemr.common.identity;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;

/**
 * Unit tests for the application entry point.
 *
 * <p>
 * These deliberately do not start a Spring context: the WAR needs a reachable
 * MySQL, Redis and Elasticsearch, none of which exist on a build agent. What is
 * worth asserting without them is the servlet-initializer wiring the Wildfly
 * deployment depends on, and the component scan the rest of the app assumes.
 */
class IdentityApplicationTests {

	@Test
	@DisplayName("configure() registers the application class as the WAR deployment source")
	void configureRegistersApplicationSource() {
		SpringApplicationBuilder builder = mock(SpringApplicationBuilder.class);
		when(builder.sources(any(Class[].class))).thenReturn(builder);

		SpringApplicationBuilder result = new IdentityApplication().configure(builder);

		assertSame(builder, result);
		verify(builder).sources(IdentityApplication.class);
	}

	@Test
	@DisplayName("instantiateBeans() exposes the IEMR helper bean")
	void instantiateBeansReturnsHelperBean() {
		assertNotNull(new IdentityApplication().instantiateBeans());
	}

	@Test
	@DisplayName("the entry point is a Spring Boot application scanning the identity packages")
	void applicationIsAnnotatedForComponentScanning() {
		assertNotNull(IdentityApplication.class.getAnnotation(SpringBootApplication.class));
		ComponentScan componentScan = IdentityApplication.class.getAnnotation(ComponentScan.class);
		assertNotNull(componentScan);
		org.junit.jupiter.api.Assertions.assertArrayEquals(new String[] { "com.iemr.common.identity" },
				componentScan.basePackages());
	}
}
