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
package com.iemr.common.identity.mapper;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

/**
 * Parses the GPS capture timestamp (epoch millis, ISO-8601 with/without a literal
 * 'Z', or the mobile client's "MMM dd, yyyy, h:mm:ss a" format).
 *
 * Attach with {@code @JsonAdapter(GpsTimestampAdapter.class)} directly on a
 * gpsTimestamp field only. Do NOT register this globally on a GsonBuilder for
 * Timestamp.class — that previously intercepted every Timestamp field in the
 * request (including dob) and silently nulled it out whenever the client's
 * format didn't match one of the patterns below.
 */
public class GpsTimestampAdapter extends TypeAdapter<Timestamp> {

	private static final Logger logger = LoggerFactory.getLogger(GpsTimestampAdapter.class);

	private static final DateTimeFormatter ISO_WITH_Z =
			DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	private static final DateTimeFormatter ISO_NO_TZ =
			DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
	private static final DateTimeFormatter CLIENT_DATE_FORMAT =
			DateTimeFormatter.ofPattern("MMM dd, yyyy, h:mm:ss a", Locale.ENGLISH);

	@Override
	public void write(JsonWriter out, Timestamp value) throws IOException {
		if (value == null) {
			out.nullValue();
		} else {
			out.value(value.getTime());
		}
	}

	@Override
	public Timestamp read(JsonReader in) throws IOException {
		if (in.peek() == JsonToken.NULL) {
			in.nextNull();
			return null;
		}
		if (in.peek() == JsonToken.NUMBER) {
			return new Timestamp(in.nextLong());
		}

		String s = in.nextString();

		// epoch millis as string
		try {
			return new Timestamp(Long.parseLong(s));
		} catch (NumberFormatException ignored) {
			// not epoch millis, try the date formats below
		}
		// ISO 8601 with Z, e.g. "2021-06-18T00:00:00.000Z"
		try {
			return Timestamp.from(Instant.parse(s));
		} catch (Exception ignored) {
			// not this format
		}
		// Mobile client format, e.g. "Jun 18, 2021, 5:30:00 AM"
		try {
			return Timestamp.valueOf(LocalDateTime.parse(s, CLIENT_DATE_FORMAT));
		} catch (Exception ignored) {
			// not this format
		}
		// ISO with literal 'Z' pattern, e.g. "2021-06-18T00:00:00.000Z" parsed as local
		try {
			return Timestamp.valueOf(LocalDateTime.parse(s, ISO_WITH_Z));
		} catch (Exception ignored) {
			// not this format
		}
		// ISO without timezone, e.g. "2021-06-18T00:00:00.000" (assume UTC)
		try {
			return Timestamp.from(LocalDateTime.parse(s, ISO_NO_TZ).toInstant(ZoneOffset.UTC));
		} catch (Exception ignored) {
			// not this format
		}

		logger.warn("GpsTimestampAdapter: unable to parse gpsTimestamp value '{}' with any known format; storing as null", s);
		return null;
	}
}
