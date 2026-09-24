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
package com.iemr.common.identity.utils.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Base64;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Tests for the static property reader used by the code that predates
 * constructor injection.
 *
 * <p>
 * Every typed accessor swallows a parse failure and falls back to a zero-ish
 * default, so a mistyped or missing property degrades silently rather than
 * failing start-up. These tests pin those defaults so a caller can tell what an
 * absent property will produce, and cover the Base64 obfuscation the datasource
 * password uses.
 */
class ConfigPropertiesTest {

	@BeforeEach
	void loadTestProperties() {
		Properties properties = new Properties();
		properties.setProperty("iemr.redis.url", "redis.internal");
		properties.setProperty("iemr.redis.port", "6379");
		properties.setProperty("iemr.session.expiry.time", "1800");
		properties.setProperty("iemr.extend.expiry.time", "true");
		properties.setProperty("padded.value", "  spaced  ");
		properties.setProperty("a.long", "9007199254740993");
		properties.setProperty("a.float", "1.5");
		properties.setProperty("not.a.number", "abc");
		properties.setProperty("plain.password", "s3cret");
		properties.setProperty("obfuscated.password", "0X10:" + Base64.getEncoder().encodeToString("s3cret".getBytes()));
		ReflectionTestUtils.setField(ConfigProperties.class, "properties", properties);
		// The cached @Value fields are static, so reset them between tests.
		ReflectionTestUtils.setField(ConfigProperties.class, "redisurl", null);
		ReflectionTestUtils.setField(ConfigProperties.class, "redisport", null);
		ReflectionTestUtils.setField(ConfigProperties.class, "sessionExpiryTime", null);
		ReflectionTestUtils.setField(ConfigProperties.class, "extendExpiryTime", null);
	}

	@Test
	@DisplayName("a configured property is returned trimmed")
	void configuredPropertyIsReturnedTrimmed() {
		assertEquals("spaced", ConfigProperties.getPropertyByName("padded.value"));
	}

	@Test
	@DisplayName("an absent property yields null rather than an empty string")
	void absentPropertyYieldsNull() {
		assertNull(ConfigProperties.getPropertyByName("no.such.property"));
	}

	@Test
	@DisplayName("a boolean property is parsed, and anything unparseable reads as false")
	void booleanPropertyIsParsed() {
		assertTrue(ConfigProperties.getBoolean("iemr.extend.expiry.time"));
		org.junit.jupiter.api.Assertions.assertFalse(ConfigProperties.getBoolean("not.a.number"));
		org.junit.jupiter.api.Assertions.assertFalse(ConfigProperties.getBoolean("no.such.property"));
	}

	@Test
	@DisplayName("numeric properties are parsed, and anything unparseable reads as zero")
	void numericPropertiesAreParsed() {
		assertEquals(6379, ConfigProperties.getInteger("iemr.redis.port"));
		assertEquals(9007199254740993L, ConfigProperties.getLong("a.long"));
		assertEquals(1.5f, ConfigProperties.getFloat("a.float"));
		assertEquals(0, ConfigProperties.getInteger("not.a.number"));
		assertEquals(0L, ConfigProperties.getLong("not.a.number"));
		assertEquals(0f, ConfigProperties.getFloat("not.a.number"));
	}

	@Test
	@DisplayName("an absent whole-number property reads as zero rather than failing")
	void absentWholeNumberPropertyReadsAsZero() {
		assertEquals(0, ConfigProperties.getInteger("no.such.property"));
		assertEquals(0L, ConfigProperties.getLong("no.such.property"));
	}

	@Test
	@DisplayName("an absent float property throws instead of falling back to zero")
	void absentFloatPropertyThrows() {
		// Float.parseFloat(null) raises NullPointerException, which the
		// NumberFormatException-only catch does not cover - unlike getInteger
		// and getLong, which do fall back to zero.
		org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class,
				() -> ConfigProperties.getFloat("no.such.property"));
	}

	@Test
	@DisplayName("the Redis connection details are read from configuration and cached")
	void redisConnectionDetailsAreReadAndCached() {
		assertEquals("redis.internal", ConfigProperties.getRedisUrl());
		assertEquals(6379, ConfigProperties.getRedisPort());
		// A second read comes from the cached static field.
		assertEquals("redis.internal", ConfigProperties.getRedisUrl());
		assertEquals(6379, ConfigProperties.getRedisPort());
	}

	@Test
	@DisplayName("the session expiry time is read from configuration and cached")
	void sessionExpiryTimeIsReadAndCached() {
		assertEquals(1800, ConfigProperties.getSessionExpiryTime());
		assertEquals(1800, ConfigProperties.getSessionExpiryTime());
	}

	@Test
	@DisplayName("the extend-expiry flag reads the session-expiry-time property, so it is effectively always off")
	void extendExpiryFlagReadsTheWrongProperty() {
		// getExtendExpiryTime() falls back to getBoolean("iemr.session.expiry.time")
		// rather than "iemr.extend.expiry.time", and a duration never parses as
		// a boolean - so sessions are never extended on read regardless of how
		// iemr.extend.expiry.time is configured.
		org.junit.jupiter.api.Assertions.assertFalse(ConfigProperties.getExtendExpiryTime());
	}

	@Test
	@DisplayName("a plain password is returned as-is")
	void plainPasswordIsReturnedAsIs() {
		assertEquals("s3cret", ConfigProperties.getPassword("plain.password"));
	}

	@Test
	@DisplayName("an obfuscated password is decoded")
	void obfuscatedPasswordIsDecoded() {
		// The 0X10: marker means the rest is Base64; without decoding, the
		// datasource would try to authenticate with the encoded text.
		assertEquals("s3cret", ConfigProperties.getPassword("obfuscated.password"));
	}

	@Test
	@DisplayName("an absent password yields null rather than an empty string")
	void absentPasswordYieldsNull() {
		assertNull(ConfigProperties.getPassword("no.such.property"));
	}

	@Test
	@DisplayName("constructing the holder loads the packaged properties file")
	void constructingTheHolderLoadsThePackagedFile() {
		ReflectionTestUtils.setField(ConfigProperties.class, "properties", null);

		new ConfigProperties();

		assertNotNull(ReflectionTestUtils.getField(ConfigProperties.class, "properties"));
	}
}
