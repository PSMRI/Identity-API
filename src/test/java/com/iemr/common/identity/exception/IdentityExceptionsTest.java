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
package com.iemr.common.identity.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the service's own exception types.
 *
 * <p>
 * These reach the caller as the {@code errorMessage} of the response envelope,
 * so the message a constructor keeps is user-visible. They also have to remain
 * checked exceptions, since the controllers rely on catching them specifically
 * to distinguish a bad request from a server fault.
 */
class IdentityExceptionsTest {

	@Test
	@DisplayName("a missing-mandatory-field failure carries the message shown to the caller")
	void missingMandatoryFieldCarriesItsMessage() {
		MissingMandatoryFieldsException thrown = new MissingMandatoryFieldsException(
				"Either of BeneficiaryID or Beneficiary Reg Id is mandatory.");

		assertEquals("Either of BeneficiaryID or Beneficiary Reg Id is mandatory.", thrown.getMessage());
		assertTrue(thrown instanceof Exception);
	}

	@Test
	@DisplayName("an illegal-action failure carries the message shown to the caller")
	void illegalActionCarriesItsMessage() {
		IllegalActionException thrown = new IllegalActionException("Beneficiary is already registered.");

		assertEquals("Beneficiary is already registered.", thrown.getMessage());
		assertTrue(thrown instanceof Exception);
	}

	@Test
	@DisplayName("a service failure adopts its cause's stack trace but not the cause itself")
	void serviceFailureAdoptsItsCausesStackTraceOnly() {
		// The two-argument constructor copies the stack trace rather than
		// calling initCause, so the original exception is not reachable from
		// the one that reaches the caller - only its frames survive.
		IllegalStateException cause = new IllegalStateException("connection refused");

		IEMRException thrown = new IEMRException("Validation error", cause);

		assertEquals("Validation error", thrown.getMessage());
		assertNull(thrown.getCause());
		org.junit.jupiter.api.Assertions.assertArrayEquals(cause.getStackTrace(), thrown.getStackTrace());
	}

	@Test
	@DisplayName("a service failure without a cause carries only its message")
	void serviceFailureWithoutACauseCarriesOnlyItsMessage() {
		IEMRException thrown = new IEMRException("Invalid family ID");

		assertEquals("Invalid family ID", thrown.getMessage());
		assertNull(thrown.getCause());
	}
}
