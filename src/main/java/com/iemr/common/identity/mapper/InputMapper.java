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
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.LongSerializationPolicy;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.iemr.common.identity.exception.IEMRException;

public class InputMapper
{
	private static final Logger logger = LoggerFactory.getLogger(InputMapper.class);

	private static GsonBuilder builder;
	private static InputMapper instance = null;

	private static final DateTimeFormatter ISO_WITH_Z =
			DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	private static final DateTimeFormatter CLIENT_DATE_FORMAT =
			DateTimeFormatter.ofPattern("MMM dd, yyyy, h:mm:ss a", Locale.ENGLISH);

	private static final TypeAdapter<Timestamp> TIMESTAMP_ADAPTER = new TypeAdapter<Timestamp>() {
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
			// ISO 8601 with Z  e.g. "2021-06-18T00:00:00.000Z"
			try { return Timestamp.from(Instant.parse(s)); } catch (Exception ignored) {}
			// Mobile client format  e.g. "Jun 18, 2021, 5:30:00 AM"
			try { return Timestamp.valueOf(LocalDateTime.parse(s, CLIENT_DATE_FORMAT)); } catch (Exception ignored) {}
			// Fallback: "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" literal Z
			try { return Timestamp.valueOf(LocalDateTime.parse(s, ISO_WITH_Z)); } catch (Exception ignored) {}
			return null;
		}
	};

	private InputMapper()
	{
		builder = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
				// .excludeFieldsWithoutExposeAnnotation()
				.serializeNulls().setLongSerializationPolicy(LongSerializationPolicy.STRING)
				.registerTypeAdapter(Timestamp.class, TIMESTAMP_ADAPTER);
	}

	public static InputMapper getInstance()
	{
		if (instance == null)
		{
			instance = new InputMapper();
		}

		return instance;
	}

	public Gson gson()
	{
		return builder.create();
	}

	public <T> T fromJson(String json, Class<T> classOfT) throws IEMRException
	{
		return builder.create().fromJson(json, classOfT);
	}

	public boolean validate(String json) throws IEMRException
	{
		// JsonElement - could be a JsonObject, a JsonArray, a JsonPrimitive or a JsonNull
		JsonElement element = new JsonParser().parse(json);

		if (element instanceof JsonObject)
		{
			logger.info("Of Type JsonObject - true!");
			return true;
		} else if (element instanceof JsonArray)
		{
			logger.info("Of Type JsonArray - true!");
			return true;
		} else if (element instanceof JsonPrimitive)
		{
			logger.info("Of Type JsonPrimitive - true!");
			return true;
		} else if (element instanceof JsonNull)
		{
			logger.info("Of Type JsonNull - true!");
			return true;
		}

		return false;
	}
}
