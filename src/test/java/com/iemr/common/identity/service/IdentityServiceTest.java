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
package com.iemr.common.identity.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.iemr.common.identity.data.rmnch.RMNCHBeneficiaryDetailsRmnch;
import com.iemr.common.identity.domain.Address;
import com.iemr.common.identity.domain.Identity;
import com.iemr.common.identity.domain.MBeneficiaryAccount;
import com.iemr.common.identity.domain.MBeneficiaryImage;
import com.iemr.common.identity.domain.MBeneficiaryaddress;
import com.iemr.common.identity.domain.MBeneficiaryconsent;
import com.iemr.common.identity.domain.MBeneficiarycontact;
import com.iemr.common.identity.domain.MBeneficiarydetail;
import com.iemr.common.identity.domain.MBeneficiaryfamilymapping;
import com.iemr.common.identity.domain.MBeneficiaryidentity;
import com.iemr.common.identity.domain.MBeneficiarymapping;
import com.iemr.common.identity.domain.MBeneficiaryregidmapping;
import com.iemr.common.identity.domain.MBeneficiaryservicemapping;
import com.iemr.common.identity.dto.BenFamilyDTO;
import com.iemr.common.identity.dto.BenIdImportDTO;
import com.iemr.common.identity.dto.BenIdentityDTO;
import com.iemr.common.identity.dto.BeneficiariesDTO;
import com.iemr.common.identity.dto.BeneficiariesPartialDTO;
import com.iemr.common.identity.dto.BeneficiaryCreateResp;
import com.iemr.common.identity.dto.IdentityDTO;
import com.iemr.common.identity.dto.IdentityEditDTO;
import com.iemr.common.identity.dto.IdentitySearchDTO;
import com.iemr.common.identity.dto.ReserveIdentityDTO;
import com.iemr.common.identity.exception.MissingMandatoryFieldsException;
import com.iemr.common.identity.mapper.BenIdImportMapper;
import com.iemr.common.identity.mapper.IdentityEditMapper;
import com.iemr.common.identity.mapper.IdentityMapper;
import com.iemr.common.identity.mapper.IdentityPartialMapper;
import com.iemr.common.identity.repo.BenAddressRepo;
import com.iemr.common.identity.repo.BenConsentRepo;
import com.iemr.common.identity.repo.BenContactRepo;
import com.iemr.common.identity.repo.BenDataAccessRepo;
import com.iemr.common.identity.repo.BenDetailRepo;
import com.iemr.common.identity.repo.BenFamilyMappingRepo;
import com.iemr.common.identity.repo.BenIdentityRepo;
import com.iemr.common.identity.repo.BenMappingRepo;
import com.iemr.common.identity.repo.BenRegIdMappingRepo;
import com.iemr.common.identity.repo.BenServiceMappingRepo;
import com.iemr.common.identity.repo.MBeneficiaryAccountRepo;
import com.iemr.common.identity.repo.MBeneficiaryImageRepo;
import com.iemr.common.identity.repo.V_BenAdvanceSearchRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBeneficiaryDetailsRmnchRepo;
import com.iemr.common.identity.utils.exception.IEMRException;

import jakarta.persistence.NoResultException;
import jakarta.persistence.QueryTimeoutException;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

@ExtendWith(MockitoExtension.class)
public class IdentityServiceTest {
	@Mock
	private JdbcTemplate jdbcTemplate;
	@Mock
	private DataSource dataSource;

	@InjectMocks
	IdentityService identityService;

	@Mock
	BenRegIdMappingRepo benRegIdMappingRepo;

	@Mock
	BenMappingRepo benMappingRepo;

	@Mock
	IdentityMapper identityMapper;

	@Mock
	BenDetailRepo detailRepo;
	
	@Mock
	BenContactRepo contactRepo;
	
	@Mock
	IdentityEditMapper editMapper; 
	
	@Mock
	BenAddressRepo addressRepo;
	
	@Mock
	MBeneficiarymapping beneficiarymapping;
	
	@Mock
	MBeneficiarydetail beneficiarydetail;
	
	@Mock
	MBeneficiaryaddress beneficiaryaddress;
	
	@Mock
	BenIdentityRepo identityRepo;
	
	@Mock
	MBeneficiaryconsent mBeneficiaryconsent;
	
	@Mock
	BenConsentRepo consentRepo;
	
	@Mock
	MBeneficiarycontact mBeneficiarycontact;
	
	@Mock
	MBeneficiaryregidmapping mBeneficiaryregidmapping;
	
	@Mock
	MBeneficiaryfamilymapping fMap;
	
	@Mock
	BenFamilyMappingRepo familyMapRepo;
	
	@Mock
	MBeneficiaryservicemapping mBeneficiaryservicemapping;
	
	@Mock
	BenServiceMappingRepo serviceMapRepo;
	
	@Mock
	MBeneficiaryidentity mBeneficiaryidentity;
	
	@Mock
	IdentityPartialMapper partialMapper;
	
	@Mock
	BenDataAccessRepo accessRepo;
	@Mock
	MBeneficiaryAccountRepo accountRepo;
	@Mock
	MBeneficiaryImageRepo imageRepo;
	@Mock
	BenMappingRepo mappingRepo;
	@Mock
	private V_BenAdvanceSearchRepo v_BenAdvanceSearchRepo;
	@Mock
	private RMNCHBeneficiaryDetailsRmnchRepo rMNCHBeneficiaryDetailsRmnchRepo;
	@Mock
	private BenIdImportMapper benIdImportMapper;
	@Test
	void getBenAdressTest()
	{
		identityService.getBenAdress();
	}
	
	
	@Test
	void getBeneficiariesTest() throws NoResultException, QueryTimeoutException, Exception {

		List<MBeneficiarymapping> list = Lists.newArrayList();

		MBeneficiarymapping mBeneficiarymapping = new MBeneficiarymapping();
		mBeneficiarymapping.setBenMapId(new BigInteger("201"));
		MBeneficiaryregidmapping mBeneficiaryregidmapping = new MBeneficiaryregidmapping();
		mBeneficiaryregidmapping.setBeneficiaryID(new BigInteger("303"));
		mBeneficiarymapping.setMBeneficiaryregidmapping(mBeneficiaryregidmapping);

		List<MBeneficiaryfamilymapping> familyList = Lists.newArrayList();
		MBeneficiaryfamilymapping familyMapping = new MBeneficiaryfamilymapping();
		familyMapping.setBenFamilyMapId(new BigInteger("808"));
		familyList.add(familyMapping);
		mBeneficiarymapping.setMBeneficiaryfamilymappings(familyList);

		List<MBeneficiaryidentity> identitylist = Lists.newArrayList();
		MBeneficiaryidentity identity = new MBeneficiaryidentity();
		identity.setBenIdentityId(new BigInteger("909"));
		identitylist.add(identity);
		mBeneficiarymapping.setMBeneficiaryidentities(identitylist);

		list.add(mBeneficiarymapping);

		BeneficiariesDTO dto = new BeneficiariesDTO();
		dto.setBenId(new BigInteger("301"));

		IdentitySearchDTO searchParams = new IdentitySearchDTO();
		searchParams.setContactNumber("9876543210");
		
		List<MBeneficiarycontact> mBenContactList=new ArrayList<>();
		MBeneficiarycontact mBeneficiarycontact2 = makeMBeneficiarycontact();
		mBenContactList.add(mBeneficiarycontact2);
		
		when(contactRepo.findByAnyPhoneNum(searchParams.getContactNumber())).thenReturn(mBenContactList);;
		List<Object[]> objArr=new ArrayList<>();
		Object[] elements = new Object[12];
		elements[0] = 987;
		elements[1] = 1;
		elements[2] = 2;
		elements[3] = 3;
		elements[4] = 45;
		elements[5] = BigInteger.valueOf(55);
		elements[6] = Long.valueOf(98);
		elements[7] = 7;
		elements[8] = 123;
		elements[9] = 9;
		elements[10] = "";
		elements[11] = Timestamp.valueOf("2011-10-02 18:48:05.123");
		objArr.add(elements);
		when(mappingRepo.getBenMappingByBenContactIdListNew(mBeneficiarycontact2.getVanSerialNo(),
				mBeneficiarycontact2.getVanID())).thenReturn(objArr);
		RMNCHBeneficiaryDetailsRmnch rmnchBenDetails=new RMNCHBeneficiaryDetailsRmnch();
		rmnchBenDetails.setHouseoldId(9l);
		rmnchBenDetails.setGuidelineId("8");
		rmnchBenDetails.setRchid("7");
		rmnchBenDetails.toString();
		
		when(rMNCHBeneficiaryDetailsRmnchRepo
				.getByRegID(((BigInteger) elements[5]).longValue())).thenReturn(rmnchBenDetails);
		MBeneficiaryaddress beneficiaryaddress1=new MBeneficiaryaddress();
		beneficiaryaddress1.setCreatedBy(null);
		beneficiaryaddress1.toString();
		when(addressRepo.getWithVanSerialNoVanID(any(),any())).thenReturn(beneficiaryaddress1);
		when(identityMapper.mBeneficiarymappingToBeneficiariesDTO(any())).thenReturn(dto);
		List<BenIdentityDTO> listOfBenIdentityDTO=new ArrayList<>();
		when(identityMapper.mBeneficiaryidentityListToBenIdentityDTOList(any())).thenReturn(listOfBenIdentityDTO);
		Object[] elements2 = new Object[4];
		elements2[0] = 987;
		elements2[1] = 1;
		elements2[2] = 2;
		//elements2[3] = 3;
		elements2[3] = Timestamp.valueOf("2011-10-02 18:48:05.123");
		List<Object[]> objArraylist=new ArrayList<>();
		objArraylist.add(elements2);
		when(v_BenAdvanceSearchRepo.getBenAbhaDetailsByBenRegID(any())).thenReturn(objArraylist);
		
		List<BeneficiariesDTO> benDTOList = identityService.getBeneficiaries(searchParams );
		Assertions.assertTrue(benDTOList.size()>=0);
	}
	

	private MBeneficiarycontact makeMBeneficiarycontact() {
		MBeneficiarycontact mBeneficiarycontact2 = new MBeneficiarycontact();
		mBeneficiarycontact2.setBenContactsID(null);
		mBeneficiarycontact2.setCreatedBy(null);
		mBeneficiarycontact2.setCreatedDate(null);
		mBeneficiarycontact2.setDeleted(null);
		mBeneficiarycontact2.setEmailId(null);
		mBeneficiarycontact2.setEmergencyContactNum(null);
		mBeneficiarycontact2.setEmergencyContactTyp(null);
		mBeneficiarycontact2.setLastModDate(null);
		mBeneficiarycontact2.setModifiedBy(null);
		mBeneficiarycontact2.setParkingPlaceID(null);
		mBeneficiarycontact2.setPhoneNum1(null);;
		mBeneficiarycontact2.setPhoneNum2(null);;
		mBeneficiarycontact2.setPhoneNum3(null);;
		mBeneficiarycontact2.setPhoneNum4(null);
		mBeneficiarycontact2.setPhoneNum5(null);;
		mBeneficiarycontact2.setPhoneTyp1(null);
		mBeneficiarycontact2.setPhoneTyp2(null);
		mBeneficiarycontact2.setPhoneTyp3(null);
		mBeneficiarycontact2.setPhoneTyp4(null);
		mBeneficiarycontact2.setPhoneTyp5(null);
		mBeneficiarycontact2.setPreferredPhoneNum(null);
		mBeneficiarycontact2.setPreferredPhoneTyp(null);
		mBeneficiarycontact2.setPreferredSMSPhoneNum(null);
		mBeneficiarycontact2.setPreferredSMSPhoneTyp(null);
		mBeneficiarycontact2.setProcessed(null);
		mBeneficiarycontact2.setReserved(null);
		mBeneficiarycontact2.setReservedById(null);;
		mBeneficiarycontact2.setReservedFor(null);;
		mBeneficiarycontact2.setReservedOn(null);;
		mBeneficiarycontact2.setVanID(654);;
		mBeneficiarycontact2.setVanSerialNo(BigInteger.valueOf(987));
		mBeneficiarycontact2.toString();
		return mBeneficiarycontact2;
	}


	@Test
	void getBeneficiariesBenIDNullTest() throws NoResultException, QueryTimeoutException, Exception
	{
		IdentitySearchDTO identitySearchDTO=new IdentitySearchDTO();
		identitySearchDTO.setBeneficiaryId(new BigInteger("101"));
		List<BeneficiariesDTO> benDTOList=identityService.getBeneficiaries(identitySearchDTO);
		assertFalse(benDTOList.size()>0);
	}

	@Test
	void getBeneficiariesBenRegIDTest() throws NoResultException, QueryTimeoutException, Exception
	{
		IdentitySearchDTO identitySearchDTO=new IdentitySearchDTO();
		identitySearchDTO.setBeneficiaryRegId(new BigInteger("201"));
		
		BeneficiariesDTO dto = new BeneficiariesDTO();
		dto.setBenId(new BigInteger("301"));
		MBeneficiarymapping mBeneficiarymapping=new MBeneficiarymapping();
		List<MBeneficiaryfamilymapping> familyList=Lists.newArrayList();
		MBeneficiaryfamilymapping familyMapping=new MBeneficiaryfamilymapping();
		familyMapping.setBenFamilyMapId(new BigInteger("808"));
		familyList.add(familyMapping);
		mBeneficiarymapping.setMBeneficiaryfamilymappings(familyList);
		
		List<MBeneficiaryidentity> identitylist=Lists.newArrayList();
		MBeneficiaryidentity identity=new MBeneficiaryidentity();
		identity.setBenIdentityId(new BigInteger("909"));
		identitylist.add(identity);
		mBeneficiarymapping.setMBeneficiaryidentities(identitylist);
		List<BeneficiariesDTO> benDTOList=identityService.getBeneficiaries(identitySearchDTO);
		assertTrue(benDTOList.size()>=0);
	}

	@Test
	void getBeneficiariesBenConTest() throws NoResultException, QueryTimeoutException, Exception
	{
		IdentitySearchDTO identitySearchDTO=new IdentitySearchDTO();
		identitySearchDTO.setContactNumber("605");
		List<BeneficiariesDTO> benDTOList=identityService.getBeneficiaries(identitySearchDTO);
		assertFalse(benDTOList.size()>0);
	}
	

	
	@Test
	void getBeneficiariesByBenIdTest() throws NoResultException, QueryTimeoutException, Exception {
		List<MBeneficiarymapping> benMapList = Lists.newArrayList();
		MBeneficiarymapping Mbeneficiary = new MBeneficiarymapping();
		Mbeneficiary.setBenMapId(new BigInteger("101"));
		MBeneficiaryregidmapping mapping = new MBeneficiaryregidmapping();
		mapping.setBeneficiaryID(new BigInteger("201"));
		Mbeneficiary.setMBeneficiaryregidmapping(mapping);
		benMapList.add(Mbeneficiary);

		BeneficiariesDTO dto = new BeneficiariesDTO();
		dto.setBenId(new BigInteger("301"));

		List<BeneficiariesDTO> bList = identityService.getBeneficiariesByBenId(Mockito.any(BigInteger.class));
		
		assertNotNull(bList);
	}
	@Test
	void getBeneficiariesByBenIdTest1() throws NoResultException, QueryTimeoutException, Exception {
		List<MBeneficiarymapping> benMapList = Lists.newArrayList();
		MBeneficiarymapping Mbeneficiary = new MBeneficiarymapping();
		Mbeneficiary.setBenMapId(new BigInteger("101"));
		MBeneficiaryregidmapping mapping = new MBeneficiaryregidmapping();
		mapping.setBeneficiaryID(new BigInteger("201"));
		Mbeneficiary.setMBeneficiaryregidmapping(mapping);
		benMapList.add(Mbeneficiary);

		BeneficiariesDTO dto = new BeneficiariesDTO();
		dto.setBenId(new BigInteger("301"));

		//doReturn(mapping).when(benRegIdMappingRepo).findByBeneficiaryID(Mockito.any(BigInteger.class));


		List<BeneficiariesDTO> bList = identityService.getBeneficiariesByBenId(Mockito.any(BigInteger.class));
		
		assertFalse(bList.size() > 0);
	}
	
	@Test
	void getBeneficiariesByPhoneNumTest() throws NoResultException, QueryTimeoutException, Exception
	{

		MBeneficiarymapping mBeneficiarymapping=new MBeneficiarymapping();
		mBeneficiarymapping.setBenMapId(new BigInteger("201"));
		List<MBeneficiarymapping> mappingList=Lists.newArrayList();
		mappingList.add(mBeneficiarymapping);
		
		//doReturn(mappingList).when(benMappingRepo).findByBeneficiaryDetailsByPhoneNumber(anyObject());
		
		BeneficiariesDTO dto = new BeneficiariesDTO();
		dto.setBenId(new BigInteger("301"));
		//doReturn(dto).when(identityMapper).mBeneficiarymappingToBeneficiariesDTO(mBeneficiarymapping);
		List<BeneficiariesDTO> dtoList=identityService.getBeneficiariesByPhoneNum(Mockito.anyString());
		assertTrue(dtoList.isEmpty());
	}
	@Test
	void getBeneficiariesByPhoneNumTest1() throws NoResultException, QueryTimeoutException, Exception
	{
		List<BeneficiariesDTO> dtoList=identityService.getBeneficiariesByPhoneNum(Mockito.anyString());
		assertFalse(dtoList.size() > 0);
	}	

		@Test
		void createIdentityTest() 
		{
			IdentityDTO identityDTO=new IdentityDTO();
			identityDTO.setIsPermAddrSameAsCurrAddr(true);
			identityDTO.setIsPermAddrSameAsEmerAddr(true);
			identityDTO.setIsEmerAddrSameAsCurrAddr(true);
			identityDTO.setIsEmerAddrSameAsPermAddr(true);
			
			IdentityDTO identityDTODup=new IdentityDTO();
			identityDTODup.setIsPermAddrSameAsCurrAddr(true);
			identityDTODup.setIsPermAddrSameAsEmerAddr(true);
			identityDTODup.setIsEmerAddrSameAsCurrAddr(true);
			identityDTODup.setIsEmerAddrSameAsPermAddr(true);
			
			identityDTO.equals(identityDTODup);
			identityDTO.hashCode();
			
			MBeneficiaryaddress benaddress=new MBeneficiaryaddress();
			benaddress.setBenAddressID(new BigInteger("101"));
			
			MBeneficiaryconsent benconsent=new MBeneficiaryconsent();
			benconsent.setBenConsentID(new BigInteger("201"));
			
			MBeneficiarycontact bencontact=new MBeneficiarycontact();
			bencontact.setBenContactsID(new BigInteger("301"));
			
			MBeneficiarydetail bendetail=new MBeneficiarydetail();
			bendetail.setBeneficiaryDetailsId(new BigInteger("401"));

			MBeneficiaryregidmapping Benregidmapping=new MBeneficiaryregidmapping();
			
			MBeneficiarymapping benMapping=new MBeneficiarymapping();
			benMapping.setBenMapId(new BigInteger("701"));
			
			
			MBeneficiaryfamilymapping familymapping=new MBeneficiaryfamilymapping();
			familymapping.setBenFamilyMapId(new BigInteger("501"));
			List<MBeneficiaryfamilymapping> fmappingList=Lists.newArrayList();
			fmappingList.add(familymapping);
			
			MBeneficiaryservicemapping servicemapping=new MBeneficiaryservicemapping();
			servicemapping.setBenServiceMapID(new BigInteger("801"));
			
			List<MBeneficiaryidentity> mIdenList=Lists.newArrayList();
			MBeneficiaryidentity mBeneficiaryidentity=new MBeneficiaryidentity();
			mBeneficiaryidentity.setBenIdentityId(new BigInteger("601"));
			mIdenList.add(mBeneficiaryidentity);
			
			List<Identity> identityList=Lists.newArrayList();
			Identity identity=new Identity();
			identity.setIdentityNo("identity"); 
			identityList.add(identity);
			identityDTO.setIdentities(identityList);

			BeneficiaryCreateResp beneficiaryCreateResp=new BeneficiaryCreateResp();
			beneficiaryCreateResp.setBenId(new BigInteger("102"));
			beneficiaryCreateResp.setBenRegId(new BigInteger("105"));
			
			Assertions.assertThrows(Exception.class, () -> identityService.createIdentity(identityDTO));
		}		
		
		@Test
		void getBeneficiariesPartialDeatilsByBenRegIdListTest1() {
			List<BigInteger> BenRegIds = new ArrayList<>();
			BenRegIds.add(BigInteger.valueOf(987));
			List<Object[]> list = new ArrayList<>();
			Object[] elements = new Object[12];
			elements[0] = 987;
			elements[1] = "";
			elements[2] = "";
			elements[3] = "";
			elements[4] = 45;
			elements[5] = 55;
			elements[6] = "";
			elements[7] = "";
			elements[8] = 123;
			elements[9] = "";
			elements[10] = "";
			elements[11] = Timestamp.valueOf("2011-10-02 18:48:05.123");
			list.add(elements);
			when(detailRepo.getWith_vanSerialNo_vanID(any(), any())).thenReturn(beneficiarydetail);
			when(benRegIdMappingRepo.getWithVanSerialNoVanID(any(), any())).thenReturn(mBeneficiaryregidmapping);
			when(mappingRepo.getBenMappingByRegIDList(BenRegIds)).thenReturn(list);
			List<BeneficiariesPartialDTO> dtoList = identityService
					.getBeneficiariesPartialDeatilsByBenRegIdList(BenRegIds);
			Assertions.assertTrue(dtoList.size() > 0);

		}
		
		@Test
		void reserveIdentity()
		{
			ReserveIdentityDTO reserveIdentityDTO=new ReserveIdentityDTO();
			reserveIdentityDTO.setReserveCount(Long.valueOf(4));
			when(benRegIdMappingRepo.countByProviderServiceMapIDAndVehicalNoOrderByBenRegIdAsc(reserveIdentityDTO.getProviderServiceMapID(), reserveIdentityDTO.getVehicalNo())).thenReturn(Long.valueOf(5));
			
			MBeneficiaryregidmapping mBeneficiaryregidmapping=new MBeneficiaryregidmapping();
			
			String res=identityService.reserveIdentity(reserveIdentityDTO);
			Assertions.assertTrue(res.equalsIgnoreCase("Successfully Completed"));
			
		}
		
		@Test
		void editIdentityTest() throws MissingMandatoryFieldsException
		{
			IdentityEditDTO identityEditDTO=new IdentityEditDTO();
			identityEditDTO.setBeneficaryId(new BigInteger("100"));
			identityEditDTO.setBeneficiaryRegId(new BigInteger("200"));
			identityEditDTO.setChangeInSelfDetails(true);
			identityEditDTO.setChangeInOtherDetails(true);
			identityEditDTO.setChangeInAssociations(true);
			identityEditDTO.setChangeInAddress(true);
			identityEditDTO.setChangeInAddress(true);
			identityEditDTO.setChangeInContacts(true);
			identityEditDTO.setChangeInIdentities(true);
			identityEditDTO.setChangeInFamilyDetails(true);	
			identityEditDTO.setChangeInBankDetails(true);
			identityEditDTO.setChangeInBenImage(true);
			identityEditDTO.hashCode();
			MBeneficiarymapping benMapping = new MBeneficiarymapping();
			benMapping.setMBeneficiarydetail(beneficiarydetail);
			benMapping.setMBeneficiaryaddress(beneficiaryaddress);
			benMapping.setMBeneficiarycontact(mBeneficiarycontact);
			MBeneficiaryImage img=new MBeneficiaryImage();
			benMapping.setMBeneficiaryImage(img);
			MBeneficiaryAccount mBeneficiaryAccount = new MBeneficiaryAccount();
			benMapping.setMBeneficiaryAccount(mBeneficiaryAccount);;
			MBeneficiarydetail mbDetl = new MBeneficiarydetail();
			mbDetl.setFamilyId("123");			
			mbDetl.setHeadOfFamily_RelationID(456);
			mbDetl.setHeadOfFamily_Relation("");
			mbDetl.setOther("other");
			mbDetl.setEmergencyRegistration(true);
			
			MBeneficiaryaddress mbAddr = new MBeneficiaryaddress();
			MBeneficiarycontact benCon = new MBeneficiarycontact();
			MBeneficiaryidentity mbenIdentity = new MBeneficiaryidentity();
			mbenIdentity.setBenIdentityId(BigInteger.valueOf(987));
			List<MBeneficiaryidentity> identities = new ArrayList<>();
			identities.add(mbenIdentity);
			List<MBeneficiaryidentity> idList = new ArrayList<>();
			MBeneficiaryfamilymapping mbenFamilyMapping = new MBeneficiaryfamilymapping();
			mbenFamilyMapping.setBenFamilyMapId(BigInteger.valueOf(9));
			List<MBeneficiaryfamilymapping> fbMaps = new ArrayList<>();
			fbMaps.add(mbenFamilyMapping);
			List<MBeneficiaryfamilymapping> fmList = new ArrayList<>();
			fmList.add(mbenFamilyMapping);
			
			MBeneficiaryAccount beneficiaryAccount = new MBeneficiaryAccount();
			when(editMapper.identityEditDTOToMBeneficiaryImage(any(IdentityEditDTO.class))).thenReturn(img);
			when(imageRepo.findIdByVanSerialNoAndVanID(any(),any())).thenReturn(Long.valueOf(987));
			when(accountRepo.save(any())).thenReturn(beneficiaryAccount);
			when(editMapper.identityEditDTOToMBeneficiaryAccount(any(IdentityEditDTO.class))).thenReturn(beneficiaryAccount);
			
			when(accountRepo.findIdByVanSerialNoAndVanID(any(), any())).thenReturn(BigInteger.valueOf(987));
			
			when(familyMapRepo.save(any())).thenReturn(mbenFamilyMapping);
			when(editMapper.identityEditDTOListToMBeneficiaryfamilymappingList(any())).thenReturn(fbMaps);

			
			when(familyMapRepo.findByBenMapIdOrderByBenFamilyMapIdAsc(benMapping.getVanSerialNo())).thenReturn(fmList);
			when(identityRepo.findByBenMapId(any())).thenReturn(idList);
			when(editMapper.identityEditDTOListToMBeneficiaryidentityList(any())).thenReturn(identities);
			when(contactRepo.findIdByVanSerialNoAndVanID(any(), any())).thenReturn(BigInteger.valueOf(987));
			when(editMapper.identityEdiDTOToMBeneficiarycontact(any())).thenReturn(benCon);
			when(addressRepo.findIdByVanSerialNoAndVanID(any(), any())).thenReturn(BigInteger.valueOf(987));
			when(editMapper.identityEditDTOToMBeneficiaryaddress(any())).thenReturn(mbAddr );
			when(detailRepo.findBenDetailsByVanSerialNoAndVanID(any(), any())).thenReturn(mbDetl);
			when(editMapper.identityEditDTOToMBeneficiarydetail(any())).thenReturn(mbDetl);
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(identityEditDTO.getBeneficiaryRegId())).thenReturn(benMapping);
			identityService.editIdentity(identityEditDTO);
		}


		@Test
	void getBeneficiariesByBenRegIdTest() throws NoResultException, QueryTimeoutException, Exception
		{	
			MBeneficiarymapping mBeneficiarymapping=new MBeneficiarymapping();
			mBeneficiarymapping.setBenMapId(new BigInteger("201"));
						
			BeneficiariesDTO dto = new BeneficiariesDTO();
			dto.setBenId(new BigInteger("301"));
			
			List<BeneficiariesDTO> dtoList=identityService.getBeneficiariesByBenRegId(Mockito.any(BigInteger.class));
			assertTrue(dtoList.isEmpty());
		}
		
		@Test
		void getBeneficiariesDeatilsByBenRegIdListTest()
		{
			List<BigInteger> benRegIds = new ArrayList<>();
			benRegIds.add(BigInteger.valueOf(987));
			List<MBeneficiarymapping> benMapIDList =Lists.newArrayList();
			MBeneficiarymapping mBeneficiarymapping=new MBeneficiarymapping();
			mBeneficiarymapping.setBenMapId(new BigInteger("201"));
			benMapIDList.add(mBeneficiarymapping);
			//doReturn(benMapIDList).when(benMappingRepo).findAllByBenRegIdOrderByBenMapIdAsc(Mockito.anyListOf(BigInteger.class));
			
			BeneficiariesDTO dto = new BeneficiariesDTO();
			dto.setBenId(new BigInteger("301"));
			
			List<BeneficiariesDTO> dtoList=identityService.getBeneficiariesDeatilsByBenRegIdList(benRegIds);

		}

		@Test
		void testcreateIdentity() throws IEMRException {
			IdentityDTO identityDTO = new IdentityDTO();
			identityDTO.setParkingPlaceId(123);
			identityDTO.toString();
			
			Identity identity = new Identity();
			
			identity.toString();
			List<Identity> listOfIdentities=new ArrayList<>();
			listOfIdentities.add(identity);
			identityDTO.setIdentities(listOfIdentities);
			
			List<BenFamilyDTO> fDTOs=new ArrayList<>();
			BenFamilyDTO benFamilyDTO = new BenFamilyDTO();
			benFamilyDTO.setBenFamilyMapId(BigInteger.valueOf(456));
			benFamilyDTO.toString();
			fDTOs.add(benFamilyDTO);
			identityDTO.setBenFamilyDTOs(fDTOs);
			identityDTO.setIsPermAddrSameAsCurrAddr(true);
			identityDTO.setIsPermAddrSameAsEmerAddr(true);
			identityDTO.setIsEmerAddrSameAsCurrAddr(true);
			identityDTO.setIsEmerAddrSameAsPermAddr(true);
			Address address = new Address();
			address.setAddressValue("Home");
			address.setAddrLine1("R1");
			address.setCountry("India");
			address.setCountryId(1);
			address.toString();
			identityDTO.setCurrentAddress(address);
			identityDTO.setEmergencyAddress(address);
			identityDTO.setPermanentAddress(address);
			List<MBeneficiaryregidmapping> findTop10000ByProvisionedAndReserved = new ArrayList<>();
			MBeneficiaryregidmapping mBeneficiaryregidmapping2 = new MBeneficiaryregidmapping();
			mBeneficiaryregidmapping2.setBeneficiaryID(BigInteger.valueOf(987));
			findTop10000ByProvisionedAndReserved.add(mBeneficiaryregidmapping2);
			when(benRegIdMappingRepo.findTop10000ByProvisionedAndReserved(false, false)).thenReturn(findTop10000ByProvisionedAndReserved);
			MBeneficiaryaddress mAddr = new MBeneficiaryaddress();
			MBeneficiaryaddress mAddrequal = new MBeneficiaryaddress();
			mAddr.equals(mAddrequal);
			mAddr.hashCode();
			when(identityMapper.identityDTOToMBeneficiaryaddress(any())).thenReturn(mAddr);
			when(addressRepo.save(mAddr)).thenReturn(mAddr);
			
			MBeneficiaryconsent beneficiaryconsent = new MBeneficiaryconsent();
			beneficiaryconsent.setBenConsentID(BigInteger.valueOf(987));
			beneficiaryconsent.toString();
			when(identityMapper.identityDTOToDefaultMBeneficiaryconsent(any(), anyBoolean(), anyBoolean())).thenReturn(beneficiaryconsent );
			when(consentRepo.save(beneficiaryconsent)).thenReturn(beneficiaryconsent);
			
			MBeneficiarycontact beneficiarycontact=new MBeneficiarycontact();
			beneficiarycontact.hashCode();
			beneficiarycontact.equals(beneficiarycontact);
			beneficiarycontact.toString();
			when(identityMapper.identityDTOToMBeneficiarycontact(any())).thenReturn(beneficiarycontact);
			when(contactRepo.save(beneficiarycontact)).thenReturn(beneficiarycontact);
			
			MBeneficiarydetail mDetl=new MBeneficiarydetail();
			MBeneficiarydetail mDet2=new MBeneficiarydetail();
			mDetl.equals(mDet2);
			mDetl.hashCode();
			mDetl.toString();
			when(identityMapper.identityDTOToMBeneficiarydetail(any())).thenReturn(mDetl);
			when(detailRepo.save(mDetl)).thenReturn(mDetl);
			
			MBeneficiaryAccount bankOBJ = new MBeneficiaryAccount();
			bankOBJ.toString();
			when(identityMapper.identityDTOToMBeneficiaryAccount(any())).thenReturn(bankOBJ);
			when(accountRepo.save(bankOBJ)).thenReturn(bankOBJ);
			
			MBeneficiaryImage benImageOBJ = new MBeneficiaryImage();
			benImageOBJ.toString();
			when(identityMapper.identityDTOToMBeneficiaryImage(any())).thenReturn(benImageOBJ);
			when(imageRepo.save(benImageOBJ)).thenReturn(benImageOBJ);
			
			
			MBeneficiarymapping benMapping = new MBeneficiarymapping();
			MBeneficiarymapping benMappingDuplicate = new MBeneficiarymapping();
			benMapping.equals(benMappingDuplicate);
			benMapping.hashCode();
			benMapping.toString();
			when(identityMapper.identityDTOToMBeneficiarymapping(any())).thenReturn(benMapping);
			when(mappingRepo.save(benMapping)).thenReturn(benMapping);
			
			List<MBeneficiaryfamilymapping> fIdenList = new ArrayList<MBeneficiaryfamilymapping>();
			MBeneficiaryfamilymapping mBeneficiaryfamilymapping = new MBeneficiaryfamilymapping();
			mBeneficiaryfamilymapping.setVanID(123);
			mBeneficiaryfamilymapping.toString();
			fIdenList.add(mBeneficiaryfamilymapping);
			when(identityMapper.identityDTOListToMBeneficiaryfamilymappingList(any())).thenReturn(fIdenList);
			
			when(familyMapRepo.saveAll(anyList())).thenReturn(fIdenList);
			
			MBeneficiaryservicemapping sMap=new MBeneficiaryservicemapping();
			sMap.toString();
			when(identityMapper.identityDTOToMBeneficiaryservicemapping(any())).thenReturn(sMap);
			when(serviceMapRepo.save(sMap)).thenReturn(sMap);
			
			List<MBeneficiaryidentity> mIdenList=new ArrayList<>();
			MBeneficiaryidentity mBeneficiaryidentity2 = new MBeneficiaryidentity();
			mBeneficiaryidentity2.toString();
			mIdenList.add(mBeneficiaryidentity2);
			when(identityMapper.identityDTOListToMBeneficiaryidentityList(any())).thenReturn(mIdenList);
			when(identityRepo.save(any())).thenReturn(mBeneficiaryidentity2);
			
			BeneficiaryCreateResp resp=new BeneficiaryCreateResp();
			resp.toString();
			resp.setBenId(BigInteger.valueOf(987));
			when(partialMapper.mBeneficiarymappingToBeneficiaryCreateResp(any())).thenReturn(resp);
			
			BeneficiaryCreateResp createIdentity = identityService.createIdentity(identityDTO);
			Assertions.assertNotNull(createIdentity);
			
		}
		
		@Test
		void testgetBeneficiaryByHealthIDAbhaAddress() throws NoResultException, QueryTimeoutException, Exception {
			
			List<BigInteger> list=new ArrayList<>();
			list.add(BigInteger.valueOf(987));
			
			
			List<String> mappinglist=new ArrayList<>();
			List<MBeneficiarymapping> mBeneficiarymappinglist=new ArrayList<>();
			MBeneficiarymapping mBeneficiarymapping = new MBeneficiarymapping();
			mBeneficiarymapping.setBenAccountID(Long.valueOf(987));
			mBeneficiarymappinglist.add(mBeneficiarymapping);
			String mappingJson = new Gson().toJson(mBeneficiarymapping);
			mappinglist.add(mappingJson);
			List<Object[]> listOdObject =new ArrayList<>();
			MBeneficiarymapping[] benMap = new MBeneficiarymapping[1];
			when(mappingRepo.getBenMappingByRegID(any())).thenReturn(listOdObject );
			when(v_BenAdvanceSearchRepo.getBenRegIDByHealthIDAbhaAddress("123")).thenReturn(list);
			identityService.getBeneficiaryByHealthIDAbhaAddress("123");
			
		}
		@Test
		void getBeneficiaryImageTest() throws ParseException {
			String req = " {\"beneficiaryRegID\": \"987\"}";
			beneficiarymapping.setBenImageId(Long.valueOf(98));
			beneficiarymapping.setVanID(7);
			when(mappingRepo.getBenImageIdByBenRegID(any())).thenReturn(beneficiarymapping);
			MBeneficiaryImage image=new MBeneficiaryImage();
			image.setBenBiometric("");
			when(imageRepo.getBenImageByBenImageID(any(),any())).thenReturn(image);
			String resp = identityService.getBeneficiaryImage(req);
			JSONParser parser = new JSONParser();  
			JSONObject json = (JSONObject) parser.parse(resp); 
			String status = (String) json.get("status");
			Assertions.assertEquals("Success", status);
		}
		@Test
		void getBeneficiaryImageExceptionTest() throws ParseException {
			String req = " {\"beneficiaryRegID\": \"987\"}";
			beneficiarymapping.setBenImageId(Long.valueOf(98));
			beneficiarymapping.setVanID(7);
			when(mappingRepo.getBenImageIdByBenRegID(any())).thenThrow(NullPointerException.class);
			
			String resp = identityService.getBeneficiaryImage(req);
			JSONParser parser = new JSONParser();  
			JSONObject json = (JSONObject) parser.parse(resp); 
			Integer status = (Integer) json.get("statusCode");
			Assertions.assertEquals(5005, status);
		}
		@Test
		void editIdentityEducationOrCommunitytest() throws MissingMandatoryFieldsException {
			IdentityEditDTO identity = new IdentityEditDTO();
			identity.setCommunityId(4);
			identity.setEducationId(3);
			identity.setBeneficiaryRegId(BigInteger.valueOf(98));
			
			MBeneficiarymapping benmapping=new MBeneficiarymapping();
			benmapping.setBenDetailsId(BigInteger.valueOf(987));
			benmapping.setVanID(5);
			when(mappingRepo.findByBenRegIdOrderByBenMapIdAsc(identity.getBeneficiaryRegId())).thenReturn(benmapping);
			identityService.editIdentityEducationOrCommunity(identity);
			Assertions.assertNotNull(benmapping);
		}
		@Test
		void testimportBenIdToLocalServer() {
			List<BenIdImportDTO> benIdImportDTOList = new ArrayList<>();
			BenIdImportDTO benIdImportDTO = new BenIdImportDTO();
			benIdImportDTOList.add(benIdImportDTO);
			ArrayList<MBeneficiaryregidmapping> list=new ArrayList<>();
			MBeneficiaryregidmapping mBeneficiaryregidmapping = new MBeneficiaryregidmapping();
			mBeneficiaryregidmapping.setBeneficiaryID(BigInteger.valueOf(987));
			list.add(mBeneficiaryregidmapping);
			when(benIdImportMapper.benIdImportDTOToMBeneficiaryregidmappings(any())).thenReturn(list);
			int[] intArray = new int[2];
			intArray[0]=1;
			intArray[1]=2;
			//when(jdbcTemplate.batchUpdate(any(), anyList())).thenReturn(intArray );
			Assertions.assertThrows(CannotGetJdbcConnectionException.class, () -> identityService.importBenIdToLocalServer(benIdImportDTOList));
		}
		
		@Test
		void testsearhBeneficiaryByFamilyId() {
			String familyId = "2";
			List<MBeneficiarydetail> benDetailsList = new ArrayList<>();
			MBeneficiarydetail mBeneficiarydetail = new MBeneficiarydetail();
			mBeneficiarydetail.setVanSerialNo(BigInteger.valueOf(9l));
			mBeneficiarydetail.setVanID(1);
			benDetailsList.add(mBeneficiarydetail);
			
			Object[] elements = new Object[4];
			elements[0] = 987;
			elements[1] = 1;
			elements[2] = 2;
			elements[3] = Timestamp.valueOf("2011-10-02 18:48:05.123");
			List<Object[]> objArraylist=new ArrayList<>();
			objArraylist.add(elements);
			when(mappingRepo.getBenMappingByBenDetailsIds(any(), any())).thenReturn(null);
			when(detailRepo.searchByFamilyId(familyId)).thenReturn(benDetailsList);
			List<BeneficiariesDTO> searhBeneficiaryByFamilyId = identityService.searhBeneficiaryByFamilyId(familyId);

			Assertions.assertTrue(searhBeneficiaryByFamilyId.isEmpty());
			}
		@Test
		void testsearhBeneficiaryByGovIdentity() {
			String govtID = "govtID";
			List<MBeneficiaryidentity> benIdentityList = new ArrayList<>();
			MBeneficiaryidentity mBeneficiaryidentity2 = new MBeneficiaryidentity();
			benIdentityList.add(mBeneficiaryidentity2);
			when(identityRepo.searchByIdentityNo(govtID)).thenReturn(benIdentityList);
			
			Object[] elements = new Object[4];
			elements[0] = 987;
			elements[1] = 1;
			elements[2] = 2;
			elements[3] = Timestamp.valueOf("2011-10-02 18:48:05.123");
			List<Object[]> objArraylist=new ArrayList<>();
			objArraylist.add(elements);
			when(mappingRepo.getBenMappingByVanSerialNo(any(), any())).thenReturn(objArraylist);
			List<BeneficiariesDTO> searhBeneficiaryByGovIdentity = identityService.searhBeneficiaryByGovIdentity(govtID);
			Assertions.assertTrue(searhBeneficiaryByGovIdentity.isEmpty());
		}
		@Test
		void testcountBeneficiaryByVillageIdAndLastModifyDate() {
			
			List<Integer> villageIDs=new ArrayList<>();
			Timestamp lastModifiedDate = Timestamp.valueOf("2011-10-02 18:48:05.123");
			when(mappingRepo.getBeneficiaryCountsByVillageIDAndLastModifyDate(any(),any())).thenReturn(9l);
			Long resp = identityService.countBeneficiaryByVillageIdAndLastModifyDate(villageIDs, lastModifiedDate);
			Assertions.assertEquals(9l, resp);
		}
}
