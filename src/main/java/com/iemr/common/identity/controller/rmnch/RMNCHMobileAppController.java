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
package com.iemr.common.identity.controller.rmnch;

import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.iemr.common.identity.service.rmnch.RmnchDataSyncService;
import com.iemr.common.identity.utils.response.OutputResponse;

import io.lettuce.core.dynamic.annotation.Param;
import io.swagger.v3.oas.annotations.Operation;


/**
 * @apiNote API will be exposed to public domain for RMNCH mobile data sync to
 *          AMRIT platform
 */
@RestController
@RequestMapping(value = "/rmnch", headers = "Authorization")
public class RMNCHMobileAppController {

	private Logger logger = LoggerFactory.getLogger(RMNCHMobileAppController.class);

	@Autowired
	@Qualifier("rmnchServiceImpl")
	RmnchDataSyncService rmnchDataSyncService;

	@PostMapping(value = "/syncDataToAmrit", consumes = "application/json", produces = "application/json")
	@Operation(summary = "Sync data to AMRIT for already regestered beneficiary with AMRIT beneficiary id ")
	public String syncDataToAmrit(@RequestBody String requestOBJ) {
		OutputResponse response = new OutputResponse();
		try {
			if (requestOBJ != null) {
				String s = rmnchDataSyncService.syncDataToAmrit(requestOBJ);
				response.setResponse(s);
			} else
				response.setError(5000, "Invalid/NULL request obj");
		} catch (Exception e) {
			logger.error("Error in RMNCH mobile data sync : {} " , e.getMessage());
			response.setError(5000, "Error in RMNCH mobile data sync : " + e);
		}
		return response.toString();

	}

	@PostMapping(value = "/syncDataToAmritByHwc", consumes = "application/json", produces = "application/json")
	@Operation(summary = "Sync data to AMRIT for already regestered beneficiary with AMRIT beneficiary id ")
	public String syncDataToAmritHwc(@RequestBody String requestOBJ) {
		OutputResponse response = new OutputResponse();
		try {
			if (requestOBJ != null) {
				String s = rmnchDataSyncService.syncDataToAmrit(requestOBJ);
				response.setResponse(s);
			} else
				response.setError(5000, "Invalid/NULL request obj");
		} catch (Exception e) {
			logger.error("Error in RMNCH mobile data sync : {} " , e.getMessage());
			response.setError(5000, "Error in RMNCH mobile data sync : " + e);
		}
		return response.toString();

	}



//	@Deprecated
	@PostMapping(value = "/getBeneficiaryDataForVillage", consumes = "application/json", produces = "application/json")
	@Operation(summary = "Get beneficiary data for given village ")
	public String getBeneficiaryData(
			@Param(value = "{\r\n" + "\"villageID\":\"Integer\",\r\n" + "\"fromDate\":\"DateTime\",\r\n"
					+ "\"toDate\":\"DateTime\",\r\n" + "\"pageNo\":\"Integer\"\r\n"
					+ "}") @RequestBody String requestOBJ,
			@RequestHeader(value = "Authorization") String authorization) {
		OutputResponse response = new OutputResponse();
		try {
			if (requestOBJ != null) {
				logger.info("request object with timestamp : " + new Timestamp(System.currentTimeMillis()) + " "
						+ requestOBJ);
				String s = rmnchDataSyncService.getBenData(requestOBJ, authorization);
				if (s != null)
					response.setResponse(s);
				else
					response.setError(5000, "No record found");
			} else
				response.setError(5000, "Invalid/NULL request obj");
		} catch (Exception e) {
			logger.error("Error in get data : " + e);
			response.setError(5000, "Error in get data : " + e);
		}
		return response.toString();

	}

	@PostMapping(value = "/getBeneficiaryDataForAsha", consumes = "application/json", produces = "application/json")
	@Operation(summary = "get beneficiary data for given village ")
	public String getBeneficiaryDataByAsha(
			@Param(value = "{\r\n" + "\"AshaId\":\"Integer\",\r\n" + "\"fromDate\":\"DateTime\",\r\n"
					+ "\"toDate\":\"DateTime\",\r\n" + "\"pageNo\":\"Integer\"\r\n"
					+ "}") @RequestBody String requestOBJ,
			@RequestHeader(value = "Authorization") String authorization) {
		OutputResponse response = new OutputResponse();
		try {
			if (requestOBJ != null) {
				logger.info("request object with timestamp : " + new Timestamp(System.currentTimeMillis()) + " "
						+ requestOBJ);
				String s = rmnchDataSyncService.getBenDataByAsha(requestOBJ, authorization);
				if (s != null)
					response.setResponse(s);
				else
					response.setError(5000, "No record found");
			} else
				response.setError(5000, "Invalid/NULL request obj");
		} catch (Exception e) {
			logger.error("Error in get data : " + e);
			response.setError(5000, "Error in get data : " + e);
		}
		return response.toString();

	}
}
