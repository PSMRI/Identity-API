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
package com.iemr.common.identity.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Timestamp;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.session.data.redis.config.ConfigureRedisAction;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;

import com.iemr.common.identity.utils.http.HTTPRequestInterceptor;
import com.iemr.common.identity.utils.redis.RedisConfig;

/**
 * Tests for the request-binding converters and the infrastructure beans.
 *
 * <p>
 * The converters sit on the MVC binding path, so a request carrying a
 * date the pattern does not match must bind as null rather than fail the whole
 * request with a 400 the caller cannot interpret. The bean definitions are
 * asserted for the specific settings the rest of the service depends on -
 * notably that Redis session configuration is a no-op, since the managed Redis
 * these deployments use rejects the CONFIG command.
 */
class ConverterAndBeanConfigTest {

	@Nested
	@DisplayName("string to timestamp binding")
	class StringToTimestampBinding {

		private final StringtoSQLDateConverter converter = new StringtoSQLDateConverter();

		@Test
		@DisplayName("a timestamp in the expected pattern is converted")
		void timestampInTheExpectedPatternIsConverted() {
			assertEquals(Timestamp.valueOf("1996-01-15 10:30:00.000"),
					converter.convert("1996-01-15 10:30:00.000"));
		}

		@ParameterizedTest
		@ValueSource(strings = { "15/01/1996", "not a date", "1996-01-15T10:30:00Z", "" })
		@DisplayName("an unparseable value binds as null rather than failing the request")
		void unparseableValueBindsAsNull(String supplied) {
			assertNull(converter.convert(supplied));
		}

		@Test
		@DisplayName("an absent value binds as null")
		void absentValueBindsAsNull() {
			assertNull(converter.convert(null));
		}
	}

	@Nested
	@DisplayName("timestamp to string binding")
	class TimestampToStringBinding {

		private final SQLDateToStringConverter converter = new SQLDateToStringConverter();

		@Test
		@DisplayName("a timestamp is rendered as a date string")
		void timestampIsRenderedAsADateString() {
			String rendered = converter.convert(Timestamp.valueOf("1996-01-15 10:30:00"));

			assertNotNull(rendered);
			assertEquals(new java.util.Date(Timestamp.valueOf("1996-01-15 10:30:00").getTime()).toString(),
					rendered);
		}

		@Test
		@DisplayName("an absent timestamp is rejected rather than rendered as text")
		void absentTimestampIsRejected() {
			assertThrows(NullPointerException.class, () -> converter.convert(null));
		}
	}

	@Nested
	@DisplayName("Redis beans")
	class RedisBeans {

		private final RedisConfig config = new RedisConfig();

		@Test
		@DisplayName("Redis session configuration is disabled because managed Redis rejects CONFIG")
		void redisSessionConfigurationIsDisabled() {
			assertSame(ConfigureRedisAction.NO_OP, config.configureRedisAction());
		}

		@Test
		@DisplayName("the template serialises values as JSON so cached users survive a restart")
		void templateSerialisesValuesAsJson() {
			RedisConnectionFactory factory = mock(RedisConnectionFactory.class);

			RedisTemplate<String, Object> template = config.redisTemplate(factory);

			assertSame(factory, template.getConnectionFactory());
			assertNotNull(template.getValueSerializer());
			assertEquals("Jackson2JsonRedisSerializer",
					template.getValueSerializer().getClass().getSimpleName());
		}
	}

	@Test
	@DisplayName("the session-refreshing interceptor is registered for every request")
	void sessionRefreshingInterceptorIsRegistered() {
		InterceptorConfig config = new InterceptorConfig();
		HTTPRequestInterceptor interceptor = mock(HTTPRequestInterceptor.class);
		ReflectionTestUtils.setField(config, "requestInterceptor", interceptor);
		InterceptorRegistry registry = new InterceptorRegistry();

		config.addInterceptors(registry);

		assertEquals(1, ((java.util.List<?>) ReflectionTestUtils.getField(registry, "registrations")).size());
	}

	@Test
	@DisplayName("the Elasticsearch client is built against the configured host and credentials")
	void elasticsearchClientIsBuiltAgainstConfiguredHost() {
		ElasticsearchConfig config = new ElasticsearchConfig();
		ReflectionTestUtils.setField(config, "esHost", "localhost");
		ReflectionTestUtils.setField(config, "esPort", 9200);
		ReflectionTestUtils.setField(config, "esUsername", "elastic");
		ReflectionTestUtils.setField(config, "esPassword", "changeme");
		ReflectionTestUtils.setField(config, "indexName", "beneficiary");

		assertNotNull(config.elasticsearchClient());
	}
}
