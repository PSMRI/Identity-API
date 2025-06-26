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
package com.iemr.common.identity.service.familyTagging;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.iemr.common.identity.data.familyTagging.BenFamilyMapping;
import com.iemr.common.identity.data.familyTagging.FamilyMembers;
import com.iemr.common.identity.domain.MBeneficiarydetail;
import com.iemr.common.identity.domain.MBeneficiarymapping;
import com.iemr.common.identity.exception.IEMRException;
import com.iemr.common.identity.repo.BenDetailRepo;
import com.iemr.common.identity.repo.BenMappingRepo;
import com.iemr.common.identity.repo.familyTag.FamilyTagRepo;
import com.iemr.common.identity.utils.mapper.InputMapper;

@Service
public class FamilyTagServiceImpl implements FamilyTagService {

	@Autowired
	private FamilyTagRepo familyTagRepo;
	@Autowired
	private BenDetailRepo benDetailRepo;
	@Autowired
	private BenMappingRepo benMappingRepo;

	@Override
	public String addTag(String request) throws IEMRException {
		try {
			BenFamilyMapping benFamilyObj = InputMapper.gson().fromJson(request, BenFamilyMapping.class);
			MBeneficiarymapping benMapping = benMappingRepo
					.getBenDetailsId(BigInteger.valueOf(benFamilyObj.getBeneficiaryRegId()));
			if (benMapping != null && benMapping.getBenDetailsId() != null && benMapping.getVanID() != null) {
				benDetailRepo.updateFamilyDetails(benFamilyObj.getFamilyId(),
						benFamilyObj.getHeadofFamily_RelationID(), benFamilyObj.getHeadofFamily_Relation(),
						benFamilyObj.getOther(), benMapping.getBenDetailsId(), benMapping.getVanID());
			} else
				throw new IEMRException(
						"Beneficiary is not found while doing family tagging. Please contact the adminstrator");
			BenFamilyMapping benFamilyRS = familyTagRepo.searchFamilyByFamilyId(benFamilyObj.getFamilyId());

			if (benFamilyRS != null) {
				if (benFamilyRS.getNoOfmembers() != null)
					benFamilyRS.setNoOfmembers(benFamilyRS.getNoOfmembers() + 1);
				else
					benFamilyRS.setNoOfmembers(1);

				benFamilyRS.setModifiedBy(benFamilyObj.getModifiedBy());

				if (Boolean.TRUE.equals(benFamilyObj.getIsHeadOfTheFamily()) && benFamilyObj.getMemberName() != null)
					benFamilyRS.setFamilyHeadName(benFamilyObj.getMemberName());

				familyTagRepo.save(benFamilyRS);

				return "Family tagging completed successfully";

			} else
				throw new IEMRException("Invalid family ID while searching family");

		} catch (Exception e) {
			throw new IEMRException("Error while tagging family to beneficiary :" + e.getLocalizedMessage());
		}

	}

	@Override
	public String doFamilyUntag(String request) throws IEMRException {

		try {
			JsonElement jsnElmnt = JsonParser.parseString(request);
			JsonObject jsnOBJ = jsnElmnt.getAsJsonObject();

			List<BenFamilyMapping> benFamilyList = new ArrayList<>();
			if (jsnOBJ != null && jsnOBJ.has("memberList") && jsnOBJ.get("memberList") != null) {
				BenFamilyMapping[] benFamilyObj = InputMapper.gson().fromJson(jsnOBJ.get("memberList"),
						BenFamilyMapping[].class);
				if (benFamilyObj != null && benFamilyObj.length > 0)
					benFamilyList = Arrays.asList(benFamilyObj);
			} else
				throw new IEMRException("Invalid request. Member-list is NULL");

			if (!benFamilyList.isEmpty()) {
				for (BenFamilyMapping obj : benFamilyList) {
					if (obj.getFamilyId() != null) {
						MBeneficiarymapping benMapping = benMappingRepo
								.getBenDetailsId(BigInteger.valueOf(obj.getBeneficiaryRegId()));

						benDetailRepo.untagFamily(obj.getModifiedBy(), benMapping.getBenDetailsId(),
								benMapping.getVanID());

						BenFamilyMapping benFamilyRS = familyTagRepo.searchFamilyByFamilyId(obj.getFamilyId());

						if (benFamilyRS != null) {
							if (benFamilyRS.getNoOfmembers() != null && benFamilyRS.getNoOfmembers() > 0)
								benFamilyRS.setNoOfmembers(benFamilyRS.getNoOfmembers() - 1);

							if (obj != null && obj.getIsHeadOfTheFamily() != null && Boolean.TRUE.equals(obj.getIsHeadOfTheFamily()))
								benFamilyRS.setFamilyHeadName("");

							benFamilyRS.setModifiedBy(obj.getModifiedBy());
							familyTagRepo.save(benFamilyRS);

						} else
							throw new IEMRException("Invalid family ID while searching family");

					} else
						throw new IEMRException("Invalid family ID / NULL");
				}

			} else
				throw new IEMRException("Invalid request object");
		} catch (Exception e) {
			throw new IEMRException("Error while untagging family :" + e.getLocalizedMessage());
		}
		return "Beneficiary untagged successfully";
	}

// edit family
	@Override
	public String editFamilyDetails(String request) throws IEMRException {

		try {
			BenFamilyMapping benFamilyObj = InputMapper.gson().fromJson(request, BenFamilyMapping.class);

			BenFamilyMapping benFamilyRS = familyTagRepo.searchFamilyByFamilyId(benFamilyObj.getFamilyId());
			if (benFamilyRS != null) {
				if (benFamilyObj.getIsHeadOfTheFamily() != null
						&& benFamilyObj.getMemberName() != null) {
					if(null != benFamilyRS.getFamilyHeadName() && benFamilyRS.getFamilyHeadName().trim().equals(benFamilyObj.getMemberName().trim()) && Boolean.TRUE.equals(!benFamilyObj.getIsHeadOfTheFamily()))
						benFamilyRS.setFamilyHeadName("");
					else 
						benFamilyRS.setFamilyHeadName(benFamilyObj.getMemberName());		
				}
				familyTagRepo.save(benFamilyRS);

			} else
				throw new IEMRException("Invalid Family ID");

			MBeneficiarymapping benMapping = benMappingRepo
					.getBenDetailsId(BigInteger.valueOf(benFamilyObj.getBeneficiaryRegId()));

			if (benMapping != null && benMapping.getBenDetailsId() != null && benMapping.getVanID() != null) {
				benDetailRepo.editFamilyDetails(benFamilyObj.getHeadofFamily_RelationID(),
						benFamilyObj.getHeadofFamily_Relation(), benFamilyObj.getOther(), benMapping.getBenDetailsId(),
						benMapping.getVanID(), benFamilyObj.getFamilyId());
			} else
				throw new IEMRException(
						"Error in getting beneficiary details. No mapping data found in DB for this ben_reg_id");

		} catch (Exception e) {
			throw new IEMRException("Error while editing family tagging details :" + e.getLocalizedMessage());
		}
		return "Beneficiary family tagging updateed successfully";
	}

	@Override
	public String searchFamily(String request) throws IEMRException {
		try {
			BenFamilyMapping benFamilyObj = InputMapper.gson().fromJson(request, BenFamilyMapping.class);
			List<BenFamilyMapping> list = null;
			if(benFamilyObj.getFamilyId() != null) 
				 list = familyTagRepo.searchFamilyWithFamilyId(benFamilyObj.getFamilyName(),
						benFamilyObj.getVillageId(), benFamilyObj.getFamilyId());
			else 
				 list = familyTagRepo.searchFamily(benFamilyObj.getFamilyName(),
						benFamilyObj.getVillageId());
			
			if (list != null && !list.isEmpty())
				return new Gson().toJson(list);
			else
				return "No records found";
		} catch (Exception e) {
			throw new IEMRException("Error while searching family :" + e.getLocalizedMessage());
		}
	}

	@Override
	public String createFamily(String request) throws IEMRException {
		try {
			BenFamilyMapping benFamilyObj = InputMapper.gson().fromJson(request, BenFamilyMapping.class);
			benFamilyObj.setFamilyId(getFamilyId(benFamilyObj.getCreatedBy()));
			benFamilyObj.setNoOfmembers(1);
			benFamilyObj = familyTagRepo.save(benFamilyObj);
			// update family details to beneficiary
			MBeneficiarymapping benMapping = benMappingRepo
					.getBenDetailsId(BigInteger.valueOf(benFamilyObj.getBeneficiaryRegId()));
			if (benMapping != null && benMapping.getBenDetailsId() != null && benMapping.getVanID() != null) {
				benDetailRepo.updateFamilyDetails(benFamilyObj.getFamilyId(),
						benFamilyObj.getHeadofFamily_RelationID(), benFamilyObj.getHeadofFamily_Relation(),
						benFamilyObj.getOther(), benMapping.getBenDetailsId(), benMapping.getVanID());
			}
			if (benFamilyObj.getBenFamilyTagId() != null)
				return new Gson().toJson(benFamilyObj);
			else
				throw new IEMRException("Error while creating family");
		} catch (Exception e) {
			throw new IEMRException("Error while creating family :" + e.getLocalizedMessage());
		}
	}

	String getFamilyId(String username) throws IEMRException {
		try {
			Integer userId = familyTagRepo.getUserId(username);
			if (userId != null) {

				// added dummy string to produce minimum 13 character timestamp
				String timestamp = String.valueOf((System.currentTimeMillis())) + "0000000000000";
				// added dummy string to produce minimum 3 character timestamp
				String userIdStr = userId + "000";

				return timestamp.substring(0, 13) + userIdStr.substring(0, 3);
			} else
				throw new Exception("invalid username");
		} catch (Exception e) {
			throw new IEMRException("Error while generating family Id :" + e.getLocalizedMessage());
		}
	}

	@Override
	public String getFamilyDetails(String request) throws IEMRException {
		try {
			BenFamilyMapping benFamilyObj = InputMapper.gson().fromJson(request, BenFamilyMapping.class);
			List<FamilyMembers> responseList = new ArrayList<>();
			if (benFamilyObj.getFamilyId() != null) {
				List<MBeneficiarydetail> list = benDetailRepo.getFamilyDetails(benFamilyObj.getFamilyId());
				addFamilyMembersToList(list,responseList);
				Map<String, Object> map = new HashMap<>();
				map.put("familyMembers", responseList);
				return new Gson().toJson(map);
			} else
				throw new IEMRException("Error while fetching family member details");
		} catch (Exception e) {
			throw new IEMRException("Error while fetching family member details :" + e.getLocalizedMessage());
		}
	}
	private void addFamilyMembersToList(List<MBeneficiarydetail> list, List<FamilyMembers> responseList) {
		StringBuilder name = new StringBuilder("");
		for (MBeneficiarydetail obj : list) {
			
			FamilyMembers famObj = new FamilyMembers();
			BigInteger benRegId = benMappingRepo.getBenRegId(obj.getBeneficiaryDetailsId(), obj.getVanID());
			if (benRegId != null)
				famObj.setMemberId(benRegId.longValue());
			name = name.append((obj.getTitle() != null ? obj.getTitle() : "")).append( " ").append(
					(obj.getFirstName() != null ? obj.getFirstName() : "")).append(" ").append(
					(obj.getLastName() != null ? obj.getLastName() : ""));
			famObj.setMemberName(name.toString());
			if (obj.getOther() != null)
				famObj.setRelationWithHead(obj.getHeadOfFamily_Relation() + "-" + obj.getOther());
			else
				famObj.setRelationWithHead(obj.getHeadOfFamily_Relation());

			responseList.add(famObj);
		}
		
	}
}
