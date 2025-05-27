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
package com.iemr.common.identity.service.rmnch;

import java.math.BigInteger;
import java.sql.Date;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.iemr.common.identity.controller.rmnch.RMNCHMobileAppController;
import com.iemr.common.identity.data.rmnch.BenHealthIDDetails;
import com.iemr.common.identity.data.rmnch.GetBenRequestHandler;
import com.iemr.common.identity.data.rmnch.NcdTbHrpData;
import com.iemr.common.identity.data.rmnch.RMNCHBeneficiaryDetailsRmnch;
import com.iemr.common.identity.data.rmnch.RMNCHBornBirthDetails;
import com.iemr.common.identity.data.rmnch.RMNCHCBACdetails;
import com.iemr.common.identity.data.rmnch.RMNCHHouseHoldDetails;
import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiaryAccount;
import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiaryImage;
import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiaryaddress;
import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiarycontact;
import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiarydetail;
import com.iemr.common.identity.data.rmnch.RMNCHMBeneficiarymapping;
import com.iemr.common.identity.repo.rmnch.RMNCHBenAccountRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBenAddressRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBenContactRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBenDetailsRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBenImageRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBeneficiaryDetailsRmnchRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBornBirthDetailsRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHCBACDetailsRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHHouseHoldDetailsRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHMBenMappingRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHMBenRegIdMapRepo;
import com.iemr.common.identity.utils.config.ConfigProperties;
import com.iemr.common.identity.utils.exception.IEMRException;
import com.iemr.common.identity.utils.http.HttpUtils;
import com.iemr.common.identity.utils.mapper.InputMapper;

@Service
@Qualifier("rmnchServiceImpl")
public class RmnchDataSyncServiceImpl implements RmnchDataSyncService {

	private Logger logger = LoggerFactory.getLogger(RMNCHMobileAppController.class);
	private ConfigProperties properties;
	@Value("${door-to-door-page-size}")
	private String door_to_door_page_size;

	@Autowired
	private RMNCHBeneficiaryDetailsRmnchRepo rMNCHBeneficiaryDetailsRmnchRepo;
	@Autowired
	private RMNCHBornBirthDetailsRepo rMNCHBornBirthDetailsRepo;
	@Autowired
	private RMNCHCBACDetailsRepo rMNCHCBACDetailsRepo;
	@Autowired
	private RMNCHHouseHoldDetailsRepo rMNCHHouseHoldDetailsRepo;

	@Autowired
	private RMNCHBenAddressRepo rMNCHBenAddressRepo;
	@Autowired
	private RMNCHMBenMappingRepo rMNCHMBenMappingRepo;
	@Autowired
	private RMNCHBenDetailsRepo rMNCHBenDetailsRepo;
	@Autowired
	private RMNCHBenAccountRepo rMNCHBenAccountRepo;
	@Autowired
	private RMNCHBenImageRepo rMNCHBenImageRepo;
	@Autowired
	private RMNCHBenContactRepo rMNCHBenContactRepo;
	@Autowired
	private RMNCHMBenRegIdMapRepo rMNCHMBenRegIdMapRepo;

	@Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
	@Override
	public String syncDataToAmrit(String requestOBJ) throws Exception {

		Map<String, Object> resultMap = new HashMap<String, Object>();

		ArrayList<Long> beneficiaryDetailsIds = new ArrayList<>();
		ArrayList<Long> bornBirthDeatilsIds = new ArrayList<>();
		ArrayList<Long> cBACDetailsIds = new ArrayList<>();
		ArrayList<Long> houseHoldDetailsIds = new ArrayList<>();

		try {
			if (requestOBJ != null && !requestOBJ.isEmpty()) {
				JsonObject jsnOBJ = new JsonObject();
				JsonParser jsnParser = new JsonParser();
				JsonElement jsnElmnt = jsnParser.parse(requestOBJ);
				jsnOBJ = jsnElmnt.getAsJsonObject();

				// other tables data saving
				// ben details RMNCH extra fields details

				BigInteger benRegID = null;

				if (jsnOBJ != null && jsnOBJ.has("beneficiaryDetails")) {
					RMNCHBeneficiaryDetailsRmnch[] objArr = InputMapper.gson()
							.fromJson(jsnOBJ.get("beneficiaryDetails"), RMNCHBeneficiaryDetailsRmnch[].class);
					List<RMNCHBeneficiaryDetailsRmnch> benDetailsExtraList = Arrays.asList(objArr);
					List<RMNCHMBeneficiarydetail> benDetailsList = new ArrayList<>();
					if (benDetailsExtraList != null && benDetailsExtraList.size() > 0) {
//						benRegID = rMNCHMBenRegIdMapRepo.getRegID(benDetailsExtraList.get(0).getBenficieryid());
//
//						if (benRegID != null) {
						
							for (RMNCHBeneficiaryDetailsRmnch obj : benDetailsExtraList) {
								benRegID = rMNCHMBenRegIdMapRepo.getRegID(obj.getBenficieryid());
								obj.setBenRegId(benRegID);
								RMNCHBeneficiaryDetailsRmnch temp = rMNCHBeneficiaryDetailsRmnchRepo
										.getByRegID(benRegID);
								if (temp != null) {
									obj.setBeneficiaryDetails_RmnchId(temp.getBeneficiaryDetails_RmnchId());
								}

								if (obj.getRelatedBeneficiaryIds() != null
										&& obj.getRelatedBeneficiaryIds().length > 0) {
									StringBuffer sb = new StringBuffer();
									int pointer = 0;
									for (Long benID : obj.getRelatedBeneficiaryIds()) {
										if (pointer == (obj.getRelatedBeneficiaryIds().length - 1))
											sb.append(benID.toString());
										else
											sb.append(benID.toString()).append(",");
									}
									obj.setRelatedBeneficiaryIdsDB(sb.toString());
								}
								RMNCHMBeneficiarydetail rmnchmBeneficiarydetail =
										rMNCHBenDetailsRepo.getByBenRegID(obj.getBenRegId());
								if (rmnchmBeneficiarydetail != null) {
									rmnchmBeneficiarydetail.setFirstName(obj.getFirstName());
									rmnchmBeneficiarydetail.setLastName(obj.getLastName());
									rmnchmBeneficiarydetail.setFatherName(obj.getFatherName());
									rmnchmBeneficiarydetail.setMotherName(obj.getMotherName());
									rmnchmBeneficiarydetail.setDob(obj.getDob());
									rmnchmBeneficiarydetail.setSpousename(obj.getSpousename());
									rmnchmBeneficiarydetail.setGender(obj.getGender());
									rmnchmBeneficiarydetail.setGenderId(obj.getGenderId());
									rmnchmBeneficiarydetail.setMaritalstatus(obj.getMaritalstatus());
									rmnchmBeneficiarydetail.setMaritalstatusId(obj.getMaritalstatusId());
									benDetailsList.add(rmnchmBeneficiarydetail);
								}

							}

							benDetailsExtraList = (ArrayList<RMNCHBeneficiaryDetailsRmnch>) rMNCHBeneficiaryDetailsRmnchRepo
									.saveAll(benDetailsExtraList);

							benDetailsExtraList.forEach((n) -> beneficiaryDetailsIds.add(n.getId()));
							// update beneficiary data in i_beneficiarydetails table
							rMNCHBenDetailsRepo.saveAll(benDetailsList);

						// born birth details
						if (jsnOBJ != null && jsnOBJ.has("bornBirthDeatils")) {
							RMNCHBornBirthDetails[] objArr1 = InputMapper.gson()
									.fromJson(jsnOBJ.get("bornBirthDeatils"), RMNCHBornBirthDetails[].class);
							List<RMNCHBornBirthDetails> bornBirthList = Arrays.asList(objArr1);
							for (RMNCHBornBirthDetails obj : bornBirthList) {
								benRegID = rMNCHMBenRegIdMapRepo.getRegID(obj.getBenficieryid());
								obj.setBenRegId(benRegID);
								RMNCHBornBirthDetails temp = rMNCHBornBirthDetailsRepo.getByRegID(benRegID);
								if (temp != null)
									obj.setBornBirthDeatilsId(temp.getBornBirthDeatilsId());
							}
							bornBirthList = (ArrayList<RMNCHBornBirthDetails>) rMNCHBornBirthDetailsRepo
									.saveAll(bornBirthList);
							// success response
							bornBirthList.forEach((n) -> bornBirthDeatilsIds.add(n.getId()));
						}
						// CBAC details
						if (jsnOBJ != null && jsnOBJ.has("cBACDetails")) {
							RMNCHCBACdetails[] objArr2 = InputMapper.gson().fromJson(jsnOBJ.get("cBACDetails"),
									RMNCHCBACdetails[].class);
							List<RMNCHCBACdetails> cbacList = Arrays.asList(objArr2);

							for (RMNCHCBACdetails obj : cbacList) {
								benRegID = rMNCHMBenRegIdMapRepo.getRegID(obj.getBenficieryid());
								obj.setBenRegId(benRegID);
								obj.setConfirmed_hrp("Not checked");
								obj.setConfirmed_ncd("Not checked");
								obj.setConfirmed_tb("Not checked");
								obj.setConfirmed_ncd_diseases("Not checked");
								obj.setDiagnosis_status("pending");
								RMNCHCBACdetails temp = rMNCHCBACDetailsRepo.getByRegID(benRegID);
								if (temp != null)
									obj.setCBACDetailsid(temp.getCBACDetailsid());
							}

							cbacList = (ArrayList<RMNCHCBACdetails>) rMNCHCBACDetailsRepo.saveAll(cbacList);
							// success response
							cbacList.forEach((n) -> cBACDetailsIds.add(n.getId()));
						}
						// house hold details
						if (jsnOBJ != null && jsnOBJ.has("houseHoldDetails")) {
							RMNCHHouseHoldDetails[] objArr3 = InputMapper.gson()
									.fromJson(jsnOBJ.get("houseHoldDetails"), RMNCHHouseHoldDetails[].class);
							List<RMNCHHouseHoldDetails> houseHoldList = Arrays.asList(objArr3);

							for (RMNCHHouseHoldDetails obj : houseHoldList) {
								RMNCHHouseHoldDetails temp = rMNCHHouseHoldDetailsRepo
										.getByHouseHoldID(obj.getHouseoldId());
								if (temp != null)
									obj.setHouseHoldDetailsId(temp.getHouseHoldDetailsId());
							}
							houseHoldList = (ArrayList<RMNCHHouseHoldDetails>) rMNCHHouseHoldDetailsRepo
									.saveAll(houseHoldList);
							// success response
							houseHoldList.forEach((n) -> houseHoldDetailsIds.add(n.getId()));
						}
					} else
						throw new Exception("invalid/empty beneficiary request data.");
				} else
					throw new Exception("invalid/empty beneficiary request data.");

			} else
				throw new Exception("invalid/empty sync request data.");
		} catch (

		Exception e) {
			throw new Exception(e.getMessage());
		}
		resultMap.put("beneficiaryDetails", beneficiaryDetailsIds);
		resultMap.put("bornBirthDeatils", bornBirthDeatilsIds);
		resultMap.put("cBACDetails", cBACDetailsIds);
		resultMap.put("houseHoldDetails", houseHoldDetailsIds);

		return new Gson().toJson(resultMap);
	}

	@Override
	public String getBenData(String requestOBJ, String authorisation) throws Exception {
		String outputResponse = null;
		int totalPage = 0;

		try {
			GetBenRequestHandler request = InputMapper.gson().fromJson(requestOBJ, GetBenRequestHandler.class);
			if (request != null && request.getVillageID() != null) {
				List<RMNCHMBeneficiaryaddress> resultSet;
				Integer pageSize = Integer.valueOf(door_to_door_page_size);
				if (request.getPageNo() != null) {
					PageRequest pr = PageRequest.of(request.getPageNo(), pageSize);
					if (request.getFromDate() != null && request.getToDate() != null) {
						Page<RMNCHMBeneficiaryaddress> p = rMNCHBenAddressRepo.getBenDataFilteredWithDateRange(
								request.getVillageID(), request.getFromDate(), request.getToDate(), pr);
						resultSet = p.getContent();
						totalPage = p.getTotalPages();
					} else {
						Page<RMNCHMBeneficiaryaddress> p = rMNCHBenAddressRepo.getBenData(request.getVillageID(), pr);
						resultSet = p.getContent();
						totalPage = p.getTotalPages();
					}
					if (resultSet != null && resultSet.size() > 0) {
						outputResponse = getMappingsForAddressIDs(resultSet, totalPage, authorisation);
					}
				} else {
					// page no not invalid
					throw new Exception("Invalid page no");
				}
			} else {
				// missing village details : village ID
				throw new Exception("Invalid/missing village details");
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return outputResponse;
	}

	@Override
	public String getBenDataByAsha(String requestOBJ, String authorisation) throws Exception {
		String outputResponse = null;
		int totalPage = 0;

		try {
			GetBenRequestHandler request = InputMapper.gson().fromJson(requestOBJ, GetBenRequestHandler.class);
			if (request != null && request.getAshaId() != null) {
				List<RMNCHMBeneficiaryaddress> resultSet;
				Integer pageSize = Integer.valueOf(door_to_door_page_size);
				if (request.getPageNo() != null) {
					String userName = rMNCHBenAddressRepo.getUserNameForAsha(request.getAshaId());
					if (userName == null || userName.isEmpty())
						throw new Exception("Asha details not found, please contact administrator");

					request.setUserName(userName);

					PageRequest pr = PageRequest.of(request.getPageNo(), pageSize);
					if (request.getFromDate() != null && request.getToDate() != null) {
						Page<RMNCHMBeneficiaryaddress> p = rMNCHBenAddressRepo.getBenDataByAshaFilteredWithDateRange(
								request.getUserName(), request.getFromDate(), request.getToDate(), pr);
						resultSet = p.getContent();
						totalPage = p.getTotalPages();
					} else {
						Page<RMNCHMBeneficiaryaddress> p = rMNCHBenAddressRepo.getBenDataByAsha(request.getUserName(),
								pr);
						resultSet = p.getContent();
						totalPage = p.getTotalPages();
					}
					if (resultSet != null && resultSet.size() > 0) {
						outputResponse = getMappingsForAddressIDs(resultSet, totalPage, authorisation);
					}
				} else {
					// page no not invalid
					throw new Exception("Invalid page no");
				}
			} else
				throw new Exception("Invalid/missing village details");

		} catch (Exception e) {
			throw new Exception(e.getMessage());
		}

		return outputResponse;
	}

	private String getMappingsForAddressIDs(List<RMNCHMBeneficiaryaddress> addressList, int totalPage,
			String authorisation) {
		RMNCHHouseHoldDetails benHouseHoldRMNCHROBJ;
		RMNCHBeneficiaryDetailsRmnch benDetailsRMNCHOBJ;
		RMNCHBornBirthDetails benBotnBirthRMNCHROBJ;
		RMNCHCBACdetails benCABCRMNCHROBJ;

		RMNCHMBeneficiarydetail benDetailsOBJ;
		RMNCHMBeneficiaryAccount benAccountOBJ;
		RMNCHMBeneficiaryImage benImageOBJ;
		RMNCHMBeneficiaryaddress benAddressOBJ;
		RMNCHMBeneficiarycontact benContactOBJ;

		Map<String, Object> resultMap;
		ArrayList<Map<String, Object>> resultList = new ArrayList<>();

		for (RMNCHMBeneficiaryaddress a : addressList) {
			// exception by-passing
			try {
				RMNCHMBeneficiarymapping m = rMNCHMBenMappingRepo.getByAddressIDAndVanID(a.getId(), a.getVanID());
				if (m != null) {
					benHouseHoldRMNCHROBJ = new RMNCHHouseHoldDetails();
					benDetailsRMNCHOBJ = new RMNCHBeneficiaryDetailsRmnch();
					benBotnBirthRMNCHROBJ = new RMNCHBornBirthDetails();
					benCABCRMNCHROBJ = new RMNCHCBACdetails();

					benDetailsOBJ = new RMNCHMBeneficiarydetail();
					benAccountOBJ = new RMNCHMBeneficiaryAccount();
					benImageOBJ = new RMNCHMBeneficiaryImage();
					benAddressOBJ = new RMNCHMBeneficiaryaddress();
					benContactOBJ = new RMNCHMBeneficiarycontact();

					if (m.getBenDetailsId() != null) {
						benDetailsOBJ = rMNCHBenDetailsRepo.getByIdAndVanID(m.getBenDetailsId(), a.getVanID());
					}
					if (m.getBenAccountID() != null) {
						benAccountOBJ = rMNCHBenAccountRepo.getByIdAndVanID(m.getBenAccountID(), a.getVanID());
					}
					if (m.getBenImageId() != null) {
						benImageOBJ = rMNCHBenImageRepo.getByIdAndVanID(m.getBenImageId().longValue(), a.getVanID());
					}
					if (m.getBenAddressId() != null) {
						benAddressOBJ = rMNCHBenAddressRepo.getByIdAndVanID(m.getBenAddressId(), a.getVanID());
					}
					if (m.getBenContactsId() != null) {
						benContactOBJ = rMNCHBenContactRepo.getByIdAndVanID(m.getBenContactsId(), a.getVanID());
					}

					BigInteger benID = null;
					if (m.getBenRegId() != null)
						benID = rMNCHMBenRegIdMapRepo.getBenIdFromRegID(m.getBenRegId().longValue());

					if (m.getBenRegId() != null) {
						benDetailsRMNCHOBJ = rMNCHBeneficiaryDetailsRmnchRepo
								.getByRegID(m.getBenRegId());
						benBotnBirthRMNCHROBJ = rMNCHBornBirthDetailsRepo.getByRegID(m.getBenRegId());

						benCABCRMNCHROBJ = rMNCHCBACDetailsRepo.getByRegID(m.getBenRegId());
						// 20-09-2021,start
						NcdTbHrpData res = getHRP_NCD_TB_SuspectedStatus(m.getBenRegId().longValue(), authorisation,
								benDetailsOBJ);
						if (res != null && benCABCRMNCHROBJ != null) {
							if (res.getConfirmed_hrp() != null)
								benCABCRMNCHROBJ.setConfirmed_hrp(res.getConfirmed_hrp());
							if (res.getConfirmed_ncd() != null)
								benCABCRMNCHROBJ.setConfirmed_ncd(res.getConfirmed_ncd());
							if (res.getConfirmed_tb() != null)
								benCABCRMNCHROBJ.setConfirmed_tb(res.getConfirmed_tb());
							if (res.getConfirmed_ncd_diseases() != null)
								benCABCRMNCHROBJ.setConfirmed_ncd_diseases(res.getConfirmed_ncd_diseases());
							if (res.getDiagnosis_status() != null)
								benCABCRMNCHROBJ.setDiagnosis_status(res.getDiagnosis_status());
							benCABCRMNCHROBJ = rMNCHCBACDetailsRepo.save(benCABCRMNCHROBJ);

						}

						// 20-09-2021,end
						if (benDetailsRMNCHOBJ != null && benDetailsRMNCHOBJ.getHouseoldId() != null)
							benHouseHoldRMNCHROBJ = rMNCHHouseHoldDetailsRepo
									.getByHouseHoldID(benDetailsRMNCHOBJ.getHouseoldId());

					}
					if (benDetailsRMNCHOBJ == null)
						benDetailsRMNCHOBJ = new RMNCHBeneficiaryDetailsRmnch();

					// new mapping 30-06-2021
					if (benDetailsOBJ.getMotherName() != null)
						benDetailsRMNCHOBJ.setMotherName(benDetailsOBJ.getMotherName());
					if (benDetailsOBJ.getLiteracyStatus() != null)
						benDetailsRMNCHOBJ.setLiteracyStatus(benDetailsOBJ.getLiteracyStatus());

					// bank
					if (benAccountOBJ.getNameOfBank() != null)
						benDetailsRMNCHOBJ.setNameOfBank(benAccountOBJ.getNameOfBank());
					if (benAccountOBJ.getBranchName() != null)
						benDetailsRMNCHOBJ.setBranchName(benAccountOBJ.getBranchName());
					if (benAccountOBJ.getIfscCode() != null)
						benDetailsRMNCHOBJ.setIfscCode(benAccountOBJ.getIfscCode());
					if (benAccountOBJ.getBankAccount() != null)
						benDetailsRMNCHOBJ.setBankAccount(benAccountOBJ.getBankAccount());

					// location
					if (benAddressOBJ.getCountyid() != null)
						benDetailsRMNCHOBJ.setCountryId(benAddressOBJ.getCountyid());
					if (benAddressOBJ.getPermCountry() != null)
						benDetailsRMNCHOBJ.setCountryName(benAddressOBJ.getPermCountry());

					if (benAddressOBJ.getStatePerm() != null)
						benDetailsRMNCHOBJ.setStateId(benAddressOBJ.getStatePerm());
					if (benAddressOBJ.getPermState() != null)
						benDetailsRMNCHOBJ.setStateName(benAddressOBJ.getPermState());

					if (benAddressOBJ.getDistrictidPerm() != null) {
						benDetailsRMNCHOBJ.setDistrictid(benAddressOBJ.getDistrictidPerm());

					}
					if (benAddressOBJ.getDistrictnamePerm() != null) {
						benDetailsRMNCHOBJ.setDistrictname(benAddressOBJ.getDistrictnamePerm());

					}

					if (benAddressOBJ.getPermSubDistrictId() != null)
						benDetailsRMNCHOBJ.setBlockId(benAddressOBJ.getPermSubDistrictId());
					if (benAddressOBJ.getPermSubDistrict() != null)
						benDetailsRMNCHOBJ.setBlockName(benAddressOBJ.getPermSubDistrict());

					if (benAddressOBJ.getVillageidPerm() != null)
						benDetailsRMNCHOBJ.setVillageId(benAddressOBJ.getVillageidPerm());
					if (benAddressOBJ.getVillagenamePerm() != null)
						benDetailsRMNCHOBJ.setVillageName(benAddressOBJ.getVillagenamePerm());

					if (benAddressOBJ.getPermServicePointId() != null)
						benDetailsRMNCHOBJ.setServicePointID(benAddressOBJ.getPermServicePointId());
					if (benAddressOBJ.getPermServicePoint() != null)
						benDetailsRMNCHOBJ.setServicePointName(benAddressOBJ.getPermServicePoint());

					if (benAddressOBJ.getPermZoneID() != null)
						benDetailsRMNCHOBJ.setZoneID(benAddressOBJ.getPermZoneID());
					if (benAddressOBJ.getPermZone() != null)
						benDetailsRMNCHOBJ.setZoneName(benAddressOBJ.getPermZone());

					if (benAddressOBJ.getPermAddrLine1() != null)
						benDetailsRMNCHOBJ.setAddressLine1(benAddressOBJ.getPermAddrLine1());
					if (benAddressOBJ.getPermAddrLine2() != null)
						benDetailsRMNCHOBJ.setAddressLine2(benAddressOBJ.getPermAddrLine2());
					if (benAddressOBJ.getPermAddrLine3() != null)
						benDetailsRMNCHOBJ.setAddressLine3(benAddressOBJ.getPermAddrLine3());

					// related benids
					if (benDetailsRMNCHOBJ.getRelatedBeneficiaryIdsDB() != null) {

						String[] relatedBenIDsString = benDetailsRMNCHOBJ.getRelatedBeneficiaryIdsDB().split(",");
						Long[] relatedBenIDs = new Long[relatedBenIDsString.length];
						int pointer = 0;
						for (String s : relatedBenIDsString) {
							relatedBenIDs[pointer] = Long.valueOf(s);
							pointer++;
						}

						benDetailsRMNCHOBJ.setRelatedBeneficiaryIds(relatedBenIDs);
					}

					if (benDetailsOBJ.getCommunity() != null)
						benDetailsRMNCHOBJ.setCommunity(benDetailsOBJ.getCommunity());
					if (benDetailsOBJ.getCommunityId() != null)
						benDetailsRMNCHOBJ.setCommunityId(benDetailsOBJ.getCommunityId());
					if (benContactOBJ.getPreferredPhoneNum() != null)
						benDetailsRMNCHOBJ.setContact_number(benContactOBJ.getPreferredPhoneNum());

					if (benDetailsOBJ.getDob() != null)
						benDetailsRMNCHOBJ.setDob(benDetailsOBJ.getDob());
					if (benDetailsOBJ.getFatherName() != null)
						benDetailsRMNCHOBJ.setFatherName(benDetailsOBJ.getFatherName());
					if (benDetailsOBJ.getFirstName() != null)
						benDetailsRMNCHOBJ.setFirstName(benDetailsOBJ.getFirstName());
					if (benDetailsOBJ.getGender() != null)
						benDetailsRMNCHOBJ.setGender(benDetailsOBJ.getGender());
					if (benDetailsOBJ.getGenderId() != null)
						benDetailsRMNCHOBJ.setGenderId(benDetailsOBJ.getGenderId());

					if (benDetailsOBJ.getMaritalstatus() != null)
						benDetailsRMNCHOBJ.setMaritalstatus(benDetailsOBJ.getMaritalstatus());
					if (benDetailsOBJ.getMaritalstatusId() != null)
						benDetailsRMNCHOBJ.setMaritalstatusId(benDetailsOBJ.getMaritalstatusId());
					if (benDetailsOBJ.getMarriageDate() != null)
						benDetailsRMNCHOBJ.setMarriageDate(benDetailsOBJ.getMarriageDate());

					if (benDetailsOBJ.getReligion() != null)
						benDetailsRMNCHOBJ.setReligion(benDetailsOBJ.getReligion());
					if (benDetailsOBJ.getReligionID() != null)
						benDetailsRMNCHOBJ.setReligionID(benDetailsOBJ.getReligionID());
					if (benDetailsOBJ.getSpousename() != null)
						benDetailsRMNCHOBJ.setSpousename(benDetailsOBJ.getSpousename());

					if (benImageOBJ != null && benImageOBJ.getUser_image() != null)
						benDetailsRMNCHOBJ.setUser_image(benImageOBJ.getUser_image());

					// new fields
					benDetailsRMNCHOBJ.setRegistrationDate(benDetailsOBJ.getCreatedDate());
					if (benID != null)
						benDetailsRMNCHOBJ.setBenficieryid(benID);

					if (benDetailsOBJ.getLastName() != null)
						benDetailsRMNCHOBJ.setLastName(benDetailsOBJ.getLastName());

					if (benDetailsRMNCHOBJ.getCreatedBy() == null)
						if (benDetailsOBJ.getCreatedBy() != null)
							benDetailsRMNCHOBJ.setCreatedBy(benDetailsOBJ.getCreatedBy());

					// age calculation
					String ageDetails = "";
					int ageVal = 0;
					String ageUnit = null;
					if (benDetailsOBJ.getDob() != null) {

						Date date = new Date(benDetailsOBJ.getDob().getTime());
						Calendar cal = Calendar.getInstance();

						cal.setTime(date);

						int year = cal.get(Calendar.YEAR);
						int month = cal.get(Calendar.MONTH) + 1;
						int day = cal.get(Calendar.DAY_OF_MONTH);

						java.time.LocalDate todayDate = java.time.LocalDate.now();
						java.time.LocalDate birthdate = java.time.LocalDate.of(year, month, day);
						Period p = Period.between(birthdate, todayDate);

						int d = p.getDays();
						int mo = p.getMonths();
						int y = p.getYears();

						if (y > 0) {
							ageDetails = y + " years - " + mo + " months";
							ageVal = y;
							ageUnit = (ageVal > 1) ? "Years" : "Year";
						} else {
							if (mo > 0) {
								ageDetails = mo + " months - " + d + " days";
								ageVal = mo;
								ageUnit = (ageVal > 1) ? "Months" : "Month";
							} else {
								ageDetails = d + " days";
								ageVal = d;
								ageUnit = (ageVal > 1) ? "Days" : "Day";
							}
						}

					}

					benDetailsRMNCHOBJ.setAgeFull(ageDetails);
					benDetailsRMNCHOBJ.setAge(ageVal);
					if (ageUnit != null)
						benDetailsRMNCHOBJ.setAge_unit(ageUnit);

					resultMap = new HashMap<>();
					if (benHouseHoldRMNCHROBJ != null)
						resultMap.put("householdDetails", benHouseHoldRMNCHROBJ);
					else
						resultMap.put("householdDetails", new HashMap<String, Object>());

					if (benBotnBirthRMNCHROBJ != null)
						resultMap.put("bornbirthDeatils", benBotnBirthRMNCHROBJ);
					else
						resultMap.put("bornbirthDeatils", new HashMap<String, Object>());

					if (benCABCRMNCHROBJ != null)
						resultMap.put("cbacDetails", benCABCRMNCHROBJ);
					else
						resultMap.put("cbacDetails", new HashMap<String, Object>());

					resultMap.put("beneficiaryDetails", benDetailsRMNCHOBJ);

					resultMap.put("houseoldId", benDetailsRMNCHOBJ.getHouseoldId());
					resultMap.put("benficieryid", benDetailsRMNCHOBJ.getBenficieryid());
					resultMap.put("BenRegId", m.getBenRegId());

					// adding asha id / created by - user id
					if (benAddressOBJ.getCreatedBy() != null) {
						Integer userID = rMNCHMBenMappingRepo.getUserIDByUserName(benAddressOBJ.getCreatedBy());
						if (userID != null && userID > 0)
							resultMap.put("ashaId", userID);
					}
					// get HealthID of ben
					if (m.getBenRegId() != null) {
						List<String> healthID = fetchHealthIdByBenRegID(m.getBenRegId().longValue(), authorisation);
						if (healthID != null)
							resultMap.put("HealthID", healthID);
					}
					resultMap.put("ProviderServiceMapID", benDetailsRMNCHOBJ.getProviderServiceMapID());
					resultMap.put("VanID", m.getVanID());

					resultList.add(resultMap);

				} else {
					// mapping not available
				}
			} catch (Exception e) {
				logger.error("error for addressID :" + a.getId() + " and vanID : " + a.getVanID());
			}
		}

		Map<String, Object> response = new HashMap<>();
		response.put("data", resultList);
		response.put("pageSize", Integer.parseInt(door_to_door_page_size));
		response.put("totalPage", totalPage);
		return new Gson().toJson(response);
	}

	public List<String> fetchHealthIdByBenRegID(Long benRegID, String authorization) {
		Map<String, Long> requestMap = new HashMap<String, Long>();
		requestMap.put("beneficiaryRegID", benRegID);
		requestMap.put("beneficiaryID", null);
		JsonParser jsnParser = new JsonParser();
		HttpUtils utils = new HttpUtils();
		List<String> result = null;
		try {
			HashMap<String, Object> header = new HashMap<String, Object>();
			header.put("Authorization", authorization);
			String responseStr = utils.post(ConfigProperties.getPropertyByName("fhir-url")
					+ ConfigProperties.getPropertyByName("getHealthID"), new Gson().toJson(requestMap), header);
			JsonElement jsnElmnt = jsnParser.parse(responseStr);
			JsonObject jsnOBJ = new JsonObject();
			jsnOBJ = jsnElmnt.getAsJsonObject();
			if (jsnOBJ.get("data") != null && jsnOBJ.get("data").getAsJsonObject().get("BenHealthDetails") != null) {
				result = new ArrayList<String>();
				BenHealthIDDetails[] ben = InputMapper.gson().fromJson(
						new Gson().toJson(jsnOBJ.get("data").getAsJsonObject().get("BenHealthDetails")),
						BenHealthIDDetails[].class);
				for (BenHealthIDDetails value : ben) {
					if (value.getHealthId() != null)
						result.add(value.getHealthId());
				}

			}

		} catch (Exception e) {
			logger.info("Error while fetching ABHA" + e.getMessage());
			return null;
		}

		return result;

	}

	public NcdTbHrpData getHRP_NCD_TB_SuspectedStatus(Long benRegID, String authorization,
			RMNCHMBeneficiarydetail benDetails) throws IEMRException {
		NcdTbHrpData response = null;
		try {

			if (benRegID != null) {
				List<Object[]> obj = rMNCHCBACDetailsRepo.getVisitDetailsbyRegID(benRegID);
				if (obj != null && obj.size() > 0) {
					response = new NcdTbHrpData();
					Long visitCode = (((BigInteger) obj.get(0)[0])).longValue();
					String visitCategory = (String) obj.get(0)[1];
					switch (visitCategory) {
					case "General OPD":
						response = getConfirmedNCD_TB_Common(benRegID, visitCode);
						response.setConfirmed_hrp(getConfirmedHRP(benRegID, visitCode, authorization, benDetails));
						break;
					case "General OPD (QC)":
						response = getConfirmedNCD_TB_Common(benRegID, visitCode);
						response.setConfirmed_hrp(getConfirmedHRP(benRegID, visitCode, authorization, benDetails));
						break;
					case "PNC":
						response = getConfirmedNCD_TB_PNC(benRegID, visitCode);
						response.setConfirmed_hrp(getConfirmedHRP(benRegID, visitCode, authorization, benDetails));
						break;
					case "ANC":
						response.setConfirmed_hrp(getConfirmedHRP(benRegID, visitCode, authorization, benDetails));
						break;
					case "NCD care":
						response = getConfirmedNCD_TB_NCD_CARE(benRegID, visitCode);
						response.setConfirmed_hrp(getConfirmedHRP(benRegID, visitCode, authorization, benDetails));
						break;
					case "NCD screening":
						response = getConfirmedNCD_TB_Common(benRegID, visitCode);
						response.setConfirmed_hrp(getConfirmedHRP(benRegID, visitCode, authorization, benDetails));
						break;
					case "COVID-19 Screening":
						response = getConfirmedNCD_TB_Common(benRegID, visitCode);
						response.setConfirmed_hrp(getConfirmedHRP(benRegID, visitCode, authorization, benDetails));
						break;

					default:
					}
				}

			}
		} catch (Exception e) {
			logger.info("Error while getting confirmed status" + e.getMessage());
			throw new IEMRException("Error while getting confirmed status" + e.getMessage());

		}
		return response;
	}

	public String getConfirmedHRP(Long benRegID, Long visitCode, String authorization,
			RMNCHMBeneficiarydetail benDetails) throws IEMRException {
		String isHRP = "Pending";
		Map<String, Long> requestMap = new HashMap<String, Long>();
		requestMap.put("benRegID", benRegID);
		requestMap.put("visitCode", visitCode);
		JsonParser jsnParser = new JsonParser();
		HttpUtils utils = new HttpUtils();
		try {
			if (benDetails != null && benDetails.getGender() != null
					&& benDetails.getGender().equalsIgnoreCase("female")) {
				HashMap<String, Object> header = new HashMap<String, Object>();
				header.put("Authorization", authorization);
				String responseStr = utils.post(
						ConfigProperties.getPropertyByName("tm-url")
								+ ConfigProperties.getPropertyByName("get-HRP-Status"),
						new Gson().toJson(requestMap), header);
				JsonElement jsnElmnt = jsnParser.parse(responseStr);
				JsonObject jsnOBJ = new JsonObject();
				jsnOBJ = jsnElmnt.getAsJsonObject();
				if (jsnOBJ.get("data") != null && jsnOBJ.get("data").getAsJsonObject().get("isHRP") != null) {
					if (jsnOBJ.get("data").getAsJsonObject().get("isHRP").getAsBoolean())
						isHRP = "Yes";
					else
						isHRP = "No";
				}

			} else
				isHRP = "No";
		} catch (Exception e) {
			logger.info("Error while getting confirmed HRP status" + e.getMessage());
			throw new IEMRException("Error while getting confirmed HRP status" + e.getMessage());

		}

		return isHRP;

	}

	public NcdTbHrpData getConfirmedNCD_TB_PNC(Long benRegID, Long visitCode) throws IEMRException {
		NcdTbHrpData response = new NcdTbHrpData();
		String dp = null;
		if (visitCode != null && benRegID != null) {
			try {
				List<Object> obj = rMNCHCBACDetailsRepo.getDiagnosisProvidedPNC(benRegID, visitCode);
				if (obj != null && obj.size() > 0 && obj.get(0) != null) {
					dp = obj.get(0).toString();
					if (dp != null) {
						String diagnosis[] = dp.split(Pattern.quote("||"));
						if (diagnosis != null && diagnosis.length > 0) {
							StringBuffer sb = new StringBuffer();
							for (String s : diagnosis) {
								if (s.equalsIgnoreCase("Tuberculosis")) {
									response.setConfirmed_tb("Yes");
									response.setDiagnosis_status("Yes");
								} else {
									if (response.getConfirmed_tb() != null
											&& !response.getConfirmed_tb().equalsIgnoreCase("Yes"))
										response.setConfirmed_tb("No");
									response.setDiagnosis_status("Yes");
								}
								if (s.equalsIgnoreCase("Diabetes mellitus") || s.equalsIgnoreCase("Hypertension")
										|| s.equalsIgnoreCase("Breast cancer")
										|| s.equalsIgnoreCase("Mental health disorder")
										|| s.equalsIgnoreCase("Oral cancer")) {

									response.setConfirmed_ncd("Yes");
									response.setDiagnosis_status("Yes");
									sb.append(s).append(",");

								} else {
									if (response.getConfirmed_ncd() != null
											&& !response.getConfirmed_ncd().equalsIgnoreCase("Yes"))
										response.setConfirmed_ncd("No");
									response.setDiagnosis_status("Yes");

								}

							}
							if (sb.length() > 1)
								response.setConfirmed_ncd_diseases(sb.substring(0, sb.length() - 1));
						}

					}
				}
			} catch (Exception e) {
				logger.info("Error while getting confirmed NCD status" + e.getMessage());
				throw new IEMRException("Error while getting confirmed NCD status" + e.getMessage());

			}
		}
		if (response != null && response.getDiagnosis_status() == null)
			response.setDiagnosis_status("Pending");
		return response;
	}

	public NcdTbHrpData getConfirmedNCD_TB_Common(Long benRegID, Long visitCode) throws IEMRException {
		NcdTbHrpData response = new NcdTbHrpData();
		String dp = null;
		if (visitCode != null && benRegID != null) {
			try {
				List<Object> obj = rMNCHCBACDetailsRepo.getDiagnosisProvidedCommon(benRegID, visitCode);
				if (obj != null && obj.size() > 0 && obj.get(0) != null) {
					dp = obj.get(0).toString();
					if (dp != null) {
						String diagnosis[] = dp.split(Pattern.quote("||"));
						if (diagnosis != null && diagnosis.length > 0) {
							StringBuffer sb = new StringBuffer();
							for (String s : diagnosis) {

								if (s.equalsIgnoreCase("Tuberculosis")) {
									response.setConfirmed_tb("Yes");
									response.setDiagnosis_status("Yes");
								} else {
									if (response.getConfirmed_tb() != null
											&& !response.getConfirmed_tb().equalsIgnoreCase("Yes"))
										response.setConfirmed_tb("No");
									response.setDiagnosis_status("Yes");
								}
								if (s.equalsIgnoreCase("Diabetes mellitus") || s.equalsIgnoreCase("Hypertension")
										|| s.equalsIgnoreCase("Breast cancer")
										|| s.equalsIgnoreCase("Mental health disorder")
										|| s.equalsIgnoreCase("Oral cancer")) {

									response.setConfirmed_ncd("Yes");
									response.setDiagnosis_status("Yes");

									sb.append(s).append(",");

								} else {
									if (response.getConfirmed_ncd() != null
											&& !response.getConfirmed_ncd().equalsIgnoreCase("Yes"))
										response.setConfirmed_ncd("No");
									response.setDiagnosis_status("Yes");
								}

							}
							if (sb.length() > 1)
								response.setConfirmed_ncd_diseases(sb.substring(0, sb.length() - 1));

						}

					}
				}
			} catch (Exception e) {
				logger.info("Error while getting confirmed NCD status" + e.getMessage());
				throw new IEMRException("Error while getting confirmed NCD status" + e.getMessage());

			}
		}
		if (response != null && response.getDiagnosis_status() == null)
			response.setDiagnosis_status("Pending");
		return response;
	}

	public NcdTbHrpData getConfirmedNCD_TB_NCD_CARE(Long benRegID, Long visitCode) throws IEMRException {
		NcdTbHrpData response = new NcdTbHrpData();
		String dp = null;
		if (visitCode != null && benRegID != null) {
			try {
				List<Object> obj = rMNCHCBACDetailsRepo.getDiagnosisProvidedNCDCare(benRegID, visitCode);
				if (obj != null && obj.size() > 0 && obj.get(0) != null) {
					dp = obj.get(0).toString();
					if (dp != null) {
						String diagnosis[] = dp.split(Pattern.quote("||"));
						if (diagnosis != null && diagnosis.length > 0) {
							StringBuffer sb = new StringBuffer();
							for (String s : diagnosis) {

								if (s.equalsIgnoreCase("Tuberculosis")) {
									response.setConfirmed_tb("Yes");
									response.setDiagnosis_status("Yes");
								} else {
									if (response.getConfirmed_tb() != null
											&& !response.getConfirmed_tb().equalsIgnoreCase("Yes"))
										response.setConfirmed_tb("No");
									response.setDiagnosis_status("Yes");
								}
								if (s.equalsIgnoreCase("Diabetes mellitus") || s.equalsIgnoreCase("Hypertension")
										|| s.equalsIgnoreCase("Breast cancer")
										|| s.equalsIgnoreCase("Mental health disorder")
										|| s.equalsIgnoreCase("Oral cancer")) {

									response.setConfirmed_ncd("Yes");
									response.setDiagnosis_status("Yes");

									sb.append(s).append(",");

								} else {
									if (response.getConfirmed_ncd() != null
											&& !response.getConfirmed_ncd().equalsIgnoreCase("Yes"))
										response.setConfirmed_ncd("No");
									if (!response.getDiagnosis_status().equalsIgnoreCase("Yes"))
										response.setDiagnosis_status("Yes");
								}

							}
							if (sb.length() > 1)
								response.setConfirmed_ncd_diseases(sb.substring(0, sb.length() - 1));

						}

					}
				}
			} catch (Exception e) {
				logger.info("Error while getting confirmed NCD status" + e.getMessage());
				throw new IEMRException("Error while getting confirmed NCD status" + e.getMessage());

			}
		}
		if (response != null && response.getDiagnosis_status() == null)
			response.setDiagnosis_status("Pending");
		return response;
	}

}
