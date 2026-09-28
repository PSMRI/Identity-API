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
package com.iemr.common.identity.utils;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Redis session infrastructure beans.
 *
 * <p>
 * These back the HTTP session store; the only thing worth asserting without a
 * context is that each bean method produces a usable instance rather than
 * sharing one, since Spring manages their lifecycle.
 */
class CommonMainTest {

	private final CommonMain config = new CommonMain();

	@Test
	@DisplayName("the Redis HTTP session configuration is provided")
	void redisHttpSessionConfigurationIsProvided() {
		assertNotNull(config.redisSession());
	}

	@Test
	@DisplayName("the session store is provided as a fresh instance per bean definition")
	void sessionStoreIsProvidedAsAFreshInstance() {
		assertNotNull(config.redisStorage());
		assertNotSame(config.redisStorage(), config.redisStorage());
	}
}
