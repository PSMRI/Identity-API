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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.gson.ExclusionStrategy;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
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
			// Timestamp fields (including dob) use Gson's default parsing here, same as
			// on vb/stoptb. The gpsTimestamp field on RMNCH entities is parsed by
			// com.iemr.common.identity.mapper.GpsTimestampAdapter via a field-level
			// @JsonAdapter annotation instead of a global registration, so it can't
			// affect any other Timestamp field.
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