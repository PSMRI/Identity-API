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
import com.iemr.common.identity.exception.IEMRException;

public class InputMapper
{
	private static final Logger logger = LoggerFactory.getLogger(InputMapper.class);

	private static GsonBuilder builder;
	private static InputMapper instance = null;

	private InputMapper()
	{
		// Timestamp fields (including dob) use Gson's default parsing here, same as
		// on vb/stoptb. The gpsTimestamp field on Address/RMNCH entities is parsed by
		// GpsTimestampAdapter via a field-level @JsonAdapter annotation instead of a
		// global registration, so it can't affect any other Timestamp field.
		builder = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
				// .excludeFieldsWithoutExposeAnnotation()
				.serializeNulls().setLongSerializationPolicy(LongSerializationPolicy.STRING);
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
