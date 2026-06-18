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
package com.iemr.common.identity.utils.mapper;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.gson.ExclusionStrategy;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.iemr.common.identity.utils.exception.IEMRException;

/**
 * @author VI314759
 *
 */
/**
 * @author VI314759
 *
 */
@Service
public class InputMapper {
	static GsonBuilder builder;
	ExclusionStrategy strategy;
	Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());

	public InputMapper() {
		if (builder == null) {
			builder = new GsonBuilder();
			builder.setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
			builder.registerTypeAdapter(Timestamp.class, new TypeAdapter<Timestamp>() {
				private final DateTimeFormatter isoWithZ = DateTimeFormatter.ISO_INSTANT;
				private final DateTimeFormatter isoNoTz = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

				@Override
				public void write(JsonWriter out, Timestamp value) throws IOException {
					if (value == null) out.nullValue();
					else out.value(value.getTime());
				}

				@Override
				public Timestamp read(JsonReader in) throws IOException {
					if (in.peek() == JsonToken.NULL) { in.nextNull(); return null; }
					if (in.peek() == JsonToken.NUMBER) return new Timestamp(in.nextLong());
					String s = in.nextString();
					// epoch millis as string
					try { return new Timestamp(Long.parseLong(s)); } catch (NumberFormatException ignored) {}
					// ISO 8601 with timezone (e.g. "2026-05-28T03:24:35.000Z")
					try { return Timestamp.from(Instant.parse(s)); } catch (Exception ignored) {}
					// ISO without timezone (e.g. "2026-05-28T03:24:35.000")
					try { return Timestamp.from(LocalDateTime.parse(s, isoNoTz).toInstant(ZoneOffset.UTC)); } catch (Exception ignored) {}
					return null;
				}
			});
		}
	}

	/**
	 * @return
	 */
	public static InputMapper gson() {
		return new InputMapper();
	}

	/**
	 * @param json
	 * @param classOfT
	 * @return
	 * @throws IEMRException
	 */
	public <T> T fromJson(String json, Class<T> classOfT) throws IEMRException {
		return builder.create().fromJson(json, classOfT);
	}

	public <T> T fromJson(JsonElement json, Class<T> classOfT) throws IEMRException {
		return builder.create().fromJson(json, classOfT);
	}

}