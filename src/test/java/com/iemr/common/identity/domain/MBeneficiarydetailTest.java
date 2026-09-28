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
package com.iemr.common.identity.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for the derived values on the beneficiary-detail entity.
 *
 * <p>
 * Field-app records arrive with an inconsistent mix of date of birth, marriage
 * date and age-at-marriage - whichever the worker was able to collect - so the
 * entity derives the missing ones. It also stores HIV status as a code while the
 * mobile app sends it as text. Both conversions are only meaningful in terms of
 * their edge cases: absent inputs, and text the app did not expect.
 */
class MBeneficiarydetailTest {

	/** The codes persisted in {@code i_beneficiarydetails.IsHIVPositive}. */
	private static final int POSITIVE_CODE = 1;
	private static final int NEGATIVE_CODE = 2;
	private static final int NOT_DISCLOSED_CODE = 3;

	@Test
	@DisplayName("age is derived from the date of birth")
	void ageIsDerivedFromDateOfBirth() {
		Timestamp dob = Timestamp.valueOf(LocalDateTime.now().minus(30, ChronoUnit.YEARS));

		assertEquals(30, MBeneficiarydetail.calculateAge(dob));
	}

	@Test
	@DisplayName("a beneficiary born today is zero years old")
	void newbornIsZeroYearsOld() {
		assertEquals(0, MBeneficiarydetail.calculateAge(Timestamp.valueOf(LocalDateTime.now())));
	}

	@Test
	@DisplayName("an unknown date of birth yields no age rather than zero")
	void unknownDateOfBirthYieldsNoAge() {
		// Zero would be indistinguishable from an infant, so a missing DOB has
		// to stay null all the way out to the API response.
		assertNull(MBeneficiarydetail.calculateAge(null));
	}

	@Test
	@DisplayName("a recorded marriage date is used as-is, even when age at marriage is also present")
	void recordedMarriageDateWins() {
		Timestamp dob = Timestamp.valueOf("1990-06-15 00:00:00");
		Timestamp marriageDate = Timestamp.valueOf("2015-02-20 00:00:00");

		assertSame(marriageDate, MBeneficiarydetail.getMarriageDateCalc(dob, marriageDate, 25));
	}

	@Test
	@DisplayName("a missing marriage date is derived from the date of birth and age at marriage")
	void marriageDateIsDerivedFromAgeAtMarriage() {
		Timestamp dob = Timestamp.valueOf("1990-06-15 00:00:00");

		Timestamp derived = MBeneficiarydetail.getMarriageDateCalc(dob, null, 25);

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(derived);
		assertEquals(2015, calendar.get(Calendar.YEAR));
		assertEquals(Calendar.JUNE, calendar.get(Calendar.MONTH));
		assertEquals(15, calendar.get(Calendar.DAY_OF_MONTH));
	}

	@Test
	@DisplayName("the marriage date stays unknown when neither the date nor the age is recorded")
	void marriageDateStaysUnknownWithoutInputs() {
		Timestamp dob = Timestamp.valueOf("1990-06-15 00:00:00");

		assertNull(MBeneficiarydetail.getMarriageDateCalc(dob, null, null));
		assertNull(MBeneficiarydetail.getMarriageDateCalc(null, null, 25));
	}

	@Test
	@DisplayName("a recorded age at marriage is used as-is")
	void recordedAgeAtMarriageWins() {
		assertEquals(22, MBeneficiarydetail.getAgeAtMarriageCalc(Timestamp.valueOf("1990-06-15 00:00:00"),
				Timestamp.valueOf("2015-02-20 00:00:00"), 22));
	}

	@Test
	@DisplayName("a missing age at marriage is derived from the two dates")
	void ageAtMarriageIsDerivedFromDates() {
		Integer derived = MBeneficiarydetail.getAgeAtMarriageCalc(Timestamp.valueOf("1990-06-15 00:00:00"),
				Timestamp.valueOf("2015-06-15 00:00:00"), null);

		assertEquals(25, derived);
	}

	@Test
	@DisplayName("the age at marriage stays unknown when a date is missing")
	void ageAtMarriageStaysUnknownWithoutBothDates() {
		assertNull(MBeneficiarydetail.getAgeAtMarriageCalc(null, Timestamp.valueOf("2015-06-15 00:00:00"), null));
		assertNull(MBeneficiarydetail.getAgeAtMarriageCalc(Timestamp.valueOf("1990-06-15 00:00:00"), null, null));
	}

	@ParameterizedTest
	@CsvSource({ "yes, 1", "YES, 1", "Yes, 1", "no, 2", "NO, 2", "No, 2" })
	@DisplayName("HIV status text maps to its stored code, case-insensitively")
	void hivStatusTextMapsToStoredCode(String status, int expectedCode) {
		assertEquals(expectedCode, MBeneficiarydetail.setIsHIVPositive(status));
	}

	@ParameterizedTest
	@ValueSource(strings = { "unknown", "", "not disclosed", "positive" })
	@DisplayName("unrecognised HIV status text falls back to not-disclosed")
	void unrecognisedHivStatusFallsBackToNotDisclosed(String status) {
		assertEquals(NOT_DISCLOSED_CODE, MBeneficiarydetail.setIsHIVPositive(status));
	}

	@ParameterizedTest
	@NullSource
	@DisplayName("a missing HIV status falls back to not-disclosed")
	void missingHivStatusFallsBackToNotDisclosed(String status) {
		assertEquals(NOT_DISCLOSED_CODE, MBeneficiarydetail.setIsHIVPositive(status));
	}

	@Test
	@DisplayName("stored HIV codes are rendered back as the text the app sent")
	void storedHivCodesRenderBackAsText() {
		assertEquals("yes", MBeneficiarydetail.getIsHIVPositive(POSITIVE_CODE));
		assertEquals("no", MBeneficiarydetail.getIsHIVPositive(NEGATIVE_CODE));
	}

	@Test
	@DisplayName("an unknown or absent HIV code renders as blank rather than a code")
	void unknownHivCodeRendersBlank() {
		assertEquals("", MBeneficiarydetail.getIsHIVPositive(NOT_DISCLOSED_CODE));
		assertEquals("", MBeneficiarydetail.getIsHIVPositive(99));
		assertEquals("", MBeneficiarydetail.getIsHIVPositive(null));
	}

	@Test
	@DisplayName("the HIV status code round-trips through the instance accessor")
	void hivStatusCodeRoundTrips() {
		MBeneficiarydetail detail = new MBeneficiarydetail();

		detail.setIsHIVPositive(MBeneficiarydetail.setIsHIVPositive("yes"));

		assertEquals(POSITIVE_CODE, detail.getIsHIVPositive());
	}
}
