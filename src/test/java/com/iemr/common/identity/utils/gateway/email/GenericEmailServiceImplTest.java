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
package com.iemr.common.identity.utils.gateway.email;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;

import org.json.JSONException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;

/**
 * Tests for the outbound notification email helper.
 *
 * <p>
 * Recipients arrive as a JSON payload, and the multi-recipient overload splits
 * a semicolon-separated {@code to} field. Getting that split wrong sends one
 * message addressed to a single malformed address, so nobody is notified and
 * nothing fails loudly.
 */
@ExtendWith(MockitoExtension.class)
class GenericEmailServiceImplTest {

	@Mock
	private JavaMailSender javaMailSender;

	private GenericEmailServiceImpl service;

	@BeforeEach
	void setUp() {
		service = new GenericEmailServiceImpl();
		service.setJavaMailSender(javaMailSender);
	}

	private static final String PAYLOAD = "{\"to\":\"nurse@example.org\",\"from\":\"amrit@example.org\","
			+ "\"subject\":\"Beneficiary registered\",\"message\":\"A new beneficiary was registered.\"}";

	private SimpleMailMessage sentMessage() {
		ArgumentCaptor<SimpleMailMessage> captor = ArgumentCaptor.forClass(SimpleMailMessage.class);
		verify(javaMailSender).send(captor.capture());
		return captor.getValue();
	}

	@Test
	@DisplayName("a templated message is sent with the addresses and text from the payload")
	void templatedMessageIsSentWithPayloadFields() throws JSONException {
		service.sendEmail(PAYLOAD, "beneficiary-registered");

		SimpleMailMessage message = sentMessage();
		assertArrayEquals(new String[] { "nurse@example.org" }, message.getTo());
		assertEquals("amrit@example.org", message.getFrom());
		assertEquals("Beneficiary registered", message.getSubject());
		assertEquals("A new beneficiary was registered.", message.getText());
	}

	@Test
	@DisplayName("a single recipient is sent to as-is")
	void singleRecipientIsSentToAsIs() throws JSONException {
		service.sendEmail(PAYLOAD);

		assertArrayEquals(new String[] { "nurse@example.org" }, sentMessage().getTo());
	}

	@Test
	@DisplayName("semicolon-separated recipients each become their own address")
	void semicolonSeparatedRecipientsEachBecomeAnAddress() throws JSONException {
		service.sendEmail(PAYLOAD.replace("nurse@example.org",
				"nurse@example.org;supervisor@example.org;admin@example.org"));

		assertArrayEquals(new String[] { "nurse@example.org", "supervisor@example.org", "admin@example.org" },
				sentMessage().getTo());
	}

	@Test
	@DisplayName("a payload missing a required field is rejected rather than sending a partial message")
	void payloadMissingARequiredFieldIsRejected() {
		assertThrows(JSONException.class, () -> service.sendEmail("{\"to\":\"nurse@example.org\"}"));
		assertThrows(JSONException.class, () -> service.sendEmail("{\"to\":\"nurse@example.org\"}", "template"));
	}

	@Test
	@DisplayName("an unparseable payload is rejected")
	void unparseablePayloadIsRejected() {
		assertThrows(JSONException.class, () -> service.sendEmail("not json at all {"));
	}
}
