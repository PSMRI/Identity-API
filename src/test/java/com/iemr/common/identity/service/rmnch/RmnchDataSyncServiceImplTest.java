package com.iemr.common.identity.service.rmnch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.gson.Gson;
import com.iemr.common.identity.data.rmnch.GetBenRequestHandler;
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

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

@ExtendWith(MockitoExtension.class)
class RmnchDataSyncServiceImplTest {
	@InjectMocks
	RmnchDataSyncServiceImpl rmnchDataSyncServiceImpl;

	@Mock
	private RMNCHBeneficiaryDetailsRmnchRepo rMNCHBeneficiaryDetailsRmnchRepo;
	@Mock
	private RMNCHBornBirthDetailsRepo rMNCHBornBirthDetailsRepo;
	@Mock
	private RMNCHCBACDetailsRepo rMNCHCBACDetailsRepo;
	@Mock
	private RMNCHHouseHoldDetailsRepo rMNCHHouseHoldDetailsRepo;

	@Mock
	private RMNCHBenAddressRepo rMNCHBenAddressRepo;
	@Mock
	private RMNCHMBenMappingRepo rMNCHMBenMappingRepo;
	@Mock
	private RMNCHBenDetailsRepo rMNCHBenDetailsRepo;
	@Mock
	private RMNCHBenAccountRepo rMNCHBenAccountRepo;
	@Mock
	private RMNCHBenImageRepo rMNCHBenImageRepo;
	@Mock
	private RMNCHBenContactRepo rMNCHBenContactRepo;
	@Mock
	private RMNCHMBenRegIdMapRepo rMNCHMBenRegIdMapRepo;
	@Mock
	RMNCHBornBirthDetails benBotnBirthRMNCHROBJ2;

	@Test
	void testSyncDataToAmrit() throws Exception {
		RMNCHBeneficiaryDetailsRmnch rMNCHBeneficiaryDetailsRmnch = new RMNCHBeneficiaryDetailsRmnch();
		RMNCHBeneficiaryDetailsRmnch rMNCHBeneficiaryDetailsRmnch2 = new RMNCHBeneficiaryDetailsRmnch();

		Long[] ids = { Long.valueOf(123), Long.valueOf(456) };
		rMNCHBeneficiaryDetailsRmnch.setRelatedBeneficiaryIds(ids);
		rMNCHBeneficiaryDetailsRmnch.hashCode();

		rMNCHBeneficiaryDetailsRmnch2.setRelatedBeneficiaryIds(ids);
		rMNCHBeneficiaryDetailsRmnch.equals(rMNCHBeneficiaryDetailsRmnch2);
		rMNCHBeneficiaryDetailsRmnch.toString();
		String json = new Gson().toJson(rMNCHBeneficiaryDetailsRmnch);

		JSONObject jsonObject = new JSONObject();

		List<String> list = new ArrayList<>();
		list.add(json);
		char s = '"';
		String t = s + "[";
		String r = "]" + s;
		jsonObject.put("beneficiaryDetails", list);

		List<String> birthlist = new ArrayList<>();
		List<RMNCHBornBirthDetails> rMNCHBornBirthDetailslist = new ArrayList<>();
		RMNCHBornBirthDetails rMNCHBornBirthDetails = new RMNCHBornBirthDetails();
		RMNCHBornBirthDetails rMNCHBornBirthDetails2 = new RMNCHBornBirthDetails();

		rMNCHBornBirthDetails.hashCode();
		rMNCHBornBirthDetails.equals(rMNCHBornBirthDetails2);
		rMNCHBornBirthDetails.toString();
		rMNCHBornBirthDetailslist.add(rMNCHBornBirthDetails);
		String birthJson = new Gson().toJson(rMNCHBornBirthDetails);
		birthlist.add(birthJson);
		jsonObject.put("bornBirthDeatils", birthlist);

		List<String> cbaclist = new ArrayList<>();
		List<RMNCHCBACdetails> rMNCHCBACdetailslist = new ArrayList<>();
		RMNCHCBACdetails rMNCHCBACdetails = new RMNCHCBACdetails();
		rMNCHCBACdetails.setBenficieryid(Long.valueOf(987));
		rMNCHCBACdetails.hashCode();
		rMNCHCBACdetails.equals(rMNCHCBACdetails);
		rMNCHCBACdetails.toString();
		rMNCHCBACdetailslist.add(rMNCHCBACdetails);
		cbaclist.add(birthJson);
		jsonObject.put("cBACDetails", cbaclist);

		List<String> householdlist = new ArrayList<>();
		List<RMNCHHouseHoldDetails> rMNCHHouseHoldDetailslist = new ArrayList<>();
		RMNCHHouseHoldDetails rMNCHHouseHoldDetails = new RMNCHHouseHoldDetails();
		rMNCHHouseHoldDetails.setHouseoldId(Long.valueOf(987));
		rMNCHHouseHoldDetails.hashCode();

		RMNCHHouseHoldDetails rMNCHHouseHoldDetails2 = new RMNCHHouseHoldDetails();
		rMNCHHouseHoldDetails2.setHouseoldId(Long.valueOf(987));

		rMNCHHouseHoldDetails.equals(rMNCHHouseHoldDetails2);
		rMNCHHouseHoldDetails.toString();
		rMNCHHouseHoldDetailslist.add(rMNCHHouseHoldDetails);
		String householdJson = new Gson().toJson(rMNCHHouseHoldDetails);
		householdlist.add(householdJson);
		jsonObject.put("houseHoldDetails", householdlist);

		String json1 = jsonObject.toString();

		String json2 = "{\"cBACDetails\":[{\"Processed\":\"N\"}],\"bornBirthDeatils\":[{\"Processed\":\"N\"}],\"houseHoldDetails\":[{\"houseoldId\":987,\"Processed\":\"N\"}],\"beneficiaryDetails\":[{\"Processed\":\"N\",\"relatedBeneficiaryIds\":[123,456]}]}";
		// String replace = json1.replace(t, "[").replace(r, "]");
		String replace2 = json2.replace("\\", "");

		when(rMNCHHouseHoldDetailsRepo.saveAll(anyList())).thenReturn(rMNCHHouseHoldDetailslist);
		when(rMNCHHouseHoldDetailsRepo.getByHouseHoldID(Long.valueOf(987))).thenReturn(rMNCHHouseHoldDetails);
		when(rMNCHCBACDetailsRepo.saveAll(anyList())).thenReturn(rMNCHCBACdetailslist);
		when(rMNCHCBACDetailsRepo.getByRegID(any())).thenReturn(rMNCHCBACdetails);
		when(rMNCHMBenRegIdMapRepo.getRegID(any())).thenReturn(Long.valueOf(987));
		when(rMNCHBornBirthDetailsRepo.saveAll(anyList())).thenReturn(rMNCHBornBirthDetailslist);
		when(rMNCHBornBirthDetailsRepo.getByRegID(any())).thenReturn(rMNCHBornBirthDetails);
		when(rMNCHMBenRegIdMapRepo.getRegID(any())).thenReturn(Long.valueOf(987));
		when(rMNCHBeneficiaryDetailsRmnchRepo.getByRegID(any())).thenReturn(rMNCHBeneficiaryDetailsRmnch);
		when(rMNCHMBenRegIdMapRepo.getRegID(any())).thenReturn(Long.valueOf(987));

		RMNCHMBeneficiarydetail rmnchBenDetails = new RMNCHMBeneficiarydetail();
		when(rMNCHBenDetailsRepo.getByBenRegID(any())).thenReturn(rmnchBenDetails);
		String syncDataToAmrit = rmnchDataSyncServiceImpl.syncDataToAmrit(replace2);
		Assertions.assertNotNull(syncDataToAmrit);
	}

	@Test
	void testgetBenDataForGeneralOPDCase() throws Exception {
		ReflectionTestUtils.setField(rmnchDataSyncServiceImpl, "door_to_door_page_size", "1");
		GetBenRequestHandler getBenRequestHandler = new GetBenRequestHandler();
		getBenRequestHandler.setAshaId(9);
		getBenRequestHandler.setVillageID(8);
		getBenRequestHandler.setPageNo(7);
		String json = new Gson().toJson(getBenRequestHandler);
		List<RMNCHMBeneficiaryaddress> list = new ArrayList<>();
		RMNCHMBeneficiaryaddress a = new RMNCHMBeneficiaryaddress();
		a.setCountyid(4);
		a.setId(BigInteger.valueOf(8));
		a.setVanID(8);

		RMNCHMBeneficiaryaddress b = new RMNCHMBeneficiaryaddress();
		b.setCountyid(4);
		b.setId(BigInteger.valueOf(8));
		b.setVanID(8);
		a.equals(b);
		a.hashCode();
		list.add(a);
		Page<RMNCHMBeneficiaryaddress> pagermnch = new PageImpl<>(list);
		when(rMNCHBenAddressRepo.getBenData(any(), any())).thenReturn(pagermnch);

		RMNCHMBeneficiarymapping m = new RMNCHMBeneficiarymapping();
		m.setBenDetailsId(BigInteger.valueOf(9));
		m.setBenAccountID(BigInteger.valueOf(9));
		m.setBenImageId(BigInteger.valueOf(9));
		m.setBenAddressId(BigInteger.valueOf(9));
		m.setBenConsentId(BigInteger.valueOf(9));
		m.setBenRegId(BigInteger.valueOf(9));
		m.toString();
		RMNCHMBeneficiarydetail benDetailsOBJ = new RMNCHMBeneficiarydetail();

		benDetailsOBJ.setCommunity("Community");
		benDetailsOBJ.setCommunityId(5);
		benDetailsOBJ.setDob(null);
		benDetailsOBJ.setFirstName("R");
		benDetailsOBJ.setFatherName("Father");
		benDetailsOBJ.setGender("M");
		benDetailsOBJ.setGenderId(1);
		benDetailsOBJ.setMaritalstatus("Single");
		benDetailsOBJ.setMaritalstatusId(3);
		benDetailsOBJ.setMarriageDate(null);
		benDetailsOBJ.setReligion("H");
		benDetailsOBJ.setReligionID(BigInteger.valueOf(9));
		benDetailsOBJ.setSpousename("S");
		benDetailsOBJ.setDob(Timestamp.valueOf("2011-10-02 18:48:05.123"));
		benDetailsOBJ.setMarriageDate(Timestamp.valueOf("2011-10-02 18:48:05.123"));
		benDetailsOBJ.toString();
		RMNCHMBeneficiaryAccount benAccountOBJ = new RMNCHMBeneficiaryAccount();
		benAccountOBJ.toString();
		RMNCHMBeneficiaryImage benImageOBJ = new RMNCHMBeneficiaryImage();
		benImageOBJ.setUser_image("image");
		benImageOBJ.toString();
		RMNCHMBeneficiaryaddress benAddressOBJ = new RMNCHMBeneficiaryaddress();
		benAddressOBJ.toString();
		RMNCHMBeneficiarycontact benContactOBJ = new RMNCHMBeneficiarycontact();
		benContactOBJ.toString();
		RMNCHBeneficiaryDetailsRmnch benDetailsRMNCHOBJ = new RMNCHBeneficiaryDetailsRmnch();
		benDetailsRMNCHOBJ.toString();

		RMNCHBornBirthDetails benBotnBirthRMNCHROBJ = new RMNCHBornBirthDetails();
		benBotnBirthRMNCHROBJ.toString();

		RMNCHCBACdetails benCABCRMNCHROBJ = new RMNCHCBACdetails();
		benCABCRMNCHROBJ.toString();

		Object[] elements = new Object[4];
		elements[0] = BigInteger.valueOf(987);
		elements[1] = "General OPD";
		elements[2] = 2;
		elements[3] = 3;
		List<Object[]> objArraylist = new ArrayList<>();
		objArraylist.add(elements);

		List<Object> listOfObject = new ArrayList<>();
		listOfObject.add("Tuberculosis||Diabetes mellitus");

		when(rMNCHBenDetailsRepo.getByIdAndVanID(m.getBenDetailsId(), a.getVanID())).thenReturn(benDetailsOBJ);
		when(rMNCHBenAccountRepo.getByIdAndVanID(m.getBenAccountID(), a.getVanID())).thenReturn(benAccountOBJ);
		when(rMNCHBenImageRepo.getByIdAndVanID(m.getBenImageId().longValue(), a.getVanID())).thenReturn(benImageOBJ);
		when(rMNCHBenAddressRepo.getByIdAndVanID(m.getBenAddressId(), a.getVanID())).thenReturn(benAddressOBJ);
		// when(rMNCHBenContactRepo.getByIdAndVanID(m.getBenContactsId(),
		// a.getVanID())).thenReturn(benContactOBJ);
		when(rMNCHMBenRegIdMapRepo.getBenIdFromRegID(m.getBenRegId().longValue())).thenReturn(BigInteger.valueOf(9));
		when(rMNCHBeneficiaryDetailsRmnchRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benDetailsRMNCHOBJ);
		when(rMNCHBornBirthDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benBotnBirthRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benCABCRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getVisitDetailsbyRegID(any())).thenReturn(objArraylist);

		when(rMNCHCBACDetailsRepo.getDiagnosisProvidedCommon(any(), any())).thenReturn(listOfObject);

		when(rMNCHMBenMappingRepo.getByAddressIDAndVanID(a.getId(), a.getVanID())).thenReturn(m);
		String resp = rmnchDataSyncServiceImpl.getBenData(json, null);
		JSONParser parser = new JSONParser();
		JSONObject jsonResp = (JSONObject) parser.parse(resp);
		String string = jsonResp.getAsString("totalPage");
		Integer valueOf = Integer.valueOf(string);
		Assertions.assertTrue(valueOf > 0);
	}

	@Test
	void testgetBenDataForGeneralOPDQCCase() throws Exception {
		ReflectionTestUtils.setField(rmnchDataSyncServiceImpl, "door_to_door_page_size", "1");
		GetBenRequestHandler getBenRequestHandler = new GetBenRequestHandler();
		getBenRequestHandler.setAshaId(9);
		getBenRequestHandler.setVillageID(8);
		getBenRequestHandler.setPageNo(7);
		String json = new Gson().toJson(getBenRequestHandler);
		List<RMNCHMBeneficiaryaddress> list = new ArrayList<>();
		RMNCHMBeneficiaryaddress a = new RMNCHMBeneficiaryaddress();
		a.setCountyid(4);
		a.setId(BigInteger.valueOf(8));
		a.setVanID(8);
		list.add(a);
		Page<RMNCHMBeneficiaryaddress> pagermnch = new PageImpl<>(list);
		when(rMNCHBenAddressRepo.getBenData(any(), any())).thenReturn(pagermnch);

		RMNCHMBeneficiarymapping m = new RMNCHMBeneficiarymapping();
		m.setBenDetailsId(BigInteger.valueOf(9));
		m.setBenAccountID(BigInteger.valueOf(9));
		m.setBenImageId(BigInteger.valueOf(9));
		m.setBenAddressId(BigInteger.valueOf(9));
		m.setBenConsentId(BigInteger.valueOf(9));
		m.setBenRegId(BigInteger.valueOf(9));
		m.toString();
		RMNCHMBeneficiarydetail benDetailsOBJ = new RMNCHMBeneficiarydetail();
		benDetailsOBJ.toString();
		RMNCHMBeneficiaryAccount benAccountOBJ = new RMNCHMBeneficiaryAccount();
		benAccountOBJ.toString();
		RMNCHMBeneficiaryImage benImageOBJ = new RMNCHMBeneficiaryImage();
		benImageOBJ.toString();
		RMNCHMBeneficiaryaddress benAddressOBJ = new RMNCHMBeneficiaryaddress();
		benAddressOBJ.toString();
		RMNCHMBeneficiarycontact benContactOBJ = new RMNCHMBeneficiarycontact();
		benContactOBJ.toString();
		RMNCHBeneficiaryDetailsRmnch benDetailsRMNCHOBJ = new RMNCHBeneficiaryDetailsRmnch();
		benDetailsRMNCHOBJ.toString();

		RMNCHBornBirthDetails benBotnBirthRMNCHROBJ = new RMNCHBornBirthDetails();
		benBotnBirthRMNCHROBJ.toString();

		RMNCHCBACdetails benCABCRMNCHROBJ = new RMNCHCBACdetails();
		benCABCRMNCHROBJ.toString();

		Object[] elements = new Object[4];
		elements[0] = BigInteger.valueOf(987);
		elements[1] = "General OPD (QC)";
		elements[2] = 2;
		elements[3] = 3;
		List<Object[]> objArraylist = new ArrayList<>();
		objArraylist.add(elements);

		List<Object> listOfObject = new ArrayList<>();
		listOfObject.add("Tuberculosis||Diabetes mellitus");
		;
		when(rMNCHBenDetailsRepo.getByIdAndVanID(m.getBenDetailsId(), a.getVanID())).thenReturn(benDetailsOBJ);
		when(rMNCHBenAccountRepo.getByIdAndVanID(m.getBenAccountID(), a.getVanID())).thenReturn(benAccountOBJ);
		when(rMNCHBenImageRepo.getByIdAndVanID(m.getBenImageId().longValue(), a.getVanID())).thenReturn(benImageOBJ);
		when(rMNCHBenAddressRepo.getByIdAndVanID(m.getBenAddressId(), a.getVanID())).thenReturn(benAddressOBJ);
		// when(rMNCHBenContactRepo.getByIdAndVanID(m.getBenContactsId(),
		// a.getVanID())).thenReturn(benContactOBJ);
		when(rMNCHMBenRegIdMapRepo.getBenIdFromRegID(m.getBenRegId().longValue())).thenReturn(BigInteger.valueOf(9));
		when(rMNCHBeneficiaryDetailsRmnchRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benDetailsRMNCHOBJ);
		when(rMNCHBornBirthDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benBotnBirthRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benCABCRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getVisitDetailsbyRegID(any())).thenReturn(objArraylist);

		when(rMNCHCBACDetailsRepo.getDiagnosisProvidedCommon(any(), any())).thenReturn(listOfObject);

		when(rMNCHMBenMappingRepo.getByAddressIDAndVanID(a.getId(), a.getVanID())).thenReturn(m);
		String resp = rmnchDataSyncServiceImpl.getBenData(json, null);
		JSONParser parser = new JSONParser();
		JSONObject jsonResp = (JSONObject) parser.parse(resp);
		String string = jsonResp.getAsString("totalPage");
		Integer valueOf = Integer.valueOf(string);
		Assertions.assertTrue(valueOf > 0);
	}

	@Test
	void testgetBenDataForPNCCase() throws Exception {
		ReflectionTestUtils.setField(rmnchDataSyncServiceImpl, "door_to_door_page_size", "1");
		GetBenRequestHandler getBenRequestHandler = new GetBenRequestHandler();
		getBenRequestHandler.setAshaId(9);
		getBenRequestHandler.setVillageID(8);
		getBenRequestHandler.setPageNo(7);
		getBenRequestHandler.hashCode();
		getBenRequestHandler.equals(getBenRequestHandler);
		getBenRequestHandler.toString();
		String json = new Gson().toJson(getBenRequestHandler);
		List<RMNCHMBeneficiaryaddress> list = new ArrayList<>();
		RMNCHMBeneficiaryaddress a = new RMNCHMBeneficiaryaddress();
		a.setCountyid(4);
		a.setId(BigInteger.valueOf(8));
		a.setVanID(8);
		a.hashCode();
		a.equals(a);
		a.toString();
		list.add(a);
		Page<RMNCHMBeneficiaryaddress> pagermnch = new PageImpl<>(list);
		when(rMNCHBenAddressRepo.getBenData(any(), any())).thenReturn(pagermnch);

		RMNCHMBeneficiarymapping m = new RMNCHMBeneficiarymapping();
		m.setBenDetailsId(BigInteger.valueOf(9));
		m.setBenAccountID(BigInteger.valueOf(9));
		m.setBenImageId(BigInteger.valueOf(9));
		m.setBenAddressId(BigInteger.valueOf(9));
		m.setBenConsentId(BigInteger.valueOf(9));
		m.setBenRegId(BigInteger.valueOf(9));
		m.hashCode();
		m.equals(m);
		m.toString();
		RMNCHMBeneficiarydetail benDetailsOBJ = new RMNCHMBeneficiarydetail();
		benDetailsOBJ.setGenderId(1);
		benDetailsOBJ.setGender("female");
		benDetailsOBJ.setMotherName("mother");
		benDetailsOBJ.setLiteracyStatus("Educated");
		benDetailsOBJ.setCommunity("Community");
		benDetailsOBJ.setCommunityId(5);
		benDetailsOBJ.setDob(null);
		benDetailsOBJ.setFirstName("R");
		benDetailsOBJ.setFatherName("Father");
		benDetailsOBJ.setGender("M");
		benDetailsOBJ.setGenderId(1);
		benDetailsOBJ.setMaritalstatus("Single");
		benDetailsOBJ.setMaritalstatusId(3);
		benDetailsOBJ.setMarriageDate(null);
		benDetailsOBJ.setReligion("H");
		benDetailsOBJ.setReligionID(BigInteger.valueOf(9));
		benDetailsOBJ.setSpousename("S");
		benDetailsOBJ.hashCode();
		benDetailsOBJ.equals(benDetailsOBJ);
		benDetailsOBJ.toString();
		RMNCHMBeneficiaryAccount benAccountOBJ = new RMNCHMBeneficiaryAccount();
		benAccountOBJ.setNameOfBank("HDFC");
		benAccountOBJ.setBranchName("Hyd");
		benAccountOBJ.setIfscCode("IFSC");
		benAccountOBJ.setBankAccount("Account");
		benAccountOBJ.hashCode();
		benAccountOBJ.equals(benAccountOBJ);
		benAccountOBJ.toString();
		RMNCHMBeneficiaryImage benImageOBJ = new RMNCHMBeneficiaryImage();
		benImageOBJ.setUser_image("image");
		benImageOBJ.hashCode();
		benImageOBJ.equals(benImageOBJ);
		benImageOBJ.toString();
		RMNCHMBeneficiaryaddress benAddressOBJ = new RMNCHMBeneficiaryaddress();
		benAddressOBJ.setCountyid(2);
		benAddressOBJ.setPermCountry("India");
		benAddressOBJ.setStatePerm(2);
		benAddressOBJ.setPermState("TS");
		benAddressOBJ.setDistrictidPerm(9);
		benAddressOBJ.setDistrictnamePerm("RR");
		benAddressOBJ.setPermSubDistrictId(8);
		benAddressOBJ.setPermSubDistrict("RR");
		benAddressOBJ.setVillageidPerm(7);
		benAddressOBJ.setVillagenamePerm("KPHB");
		benAddressOBJ.setPermServicePointId(7);
		benAddressOBJ.setPermServicePoint("Service");
		benAddressOBJ.setPermZoneID(5);
		benAddressOBJ.setPermZone("EAST");
		benAddressOBJ.setPermAddrLine1("A1");
		benAddressOBJ.setPermAddrLine2("A2");
		benAddressOBJ.setPermAddrLine3("A3");
		benAddressOBJ.toString();
		RMNCHMBeneficiarycontact benContactOBJ = new RMNCHMBeneficiarycontact();
		benContactOBJ.setPreferredPhoneNum("9876543210");
		benContactOBJ.toString();
		RMNCHBeneficiaryDetailsRmnch benDetailsRMNCHOBJ = new RMNCHBeneficiaryDetailsRmnch();
		benDetailsRMNCHOBJ.setRelatedBeneficiaryIdsDB("RelatedDB");

		benDetailsRMNCHOBJ.setHouseoldId(9l);
		benDetailsRMNCHOBJ.toString();

		RMNCHBornBirthDetails benBotnBirthRMNCHROBJ = new RMNCHBornBirthDetails();
		benBotnBirthRMNCHROBJ.toString();

		RMNCHCBACdetails benCABCRMNCHROBJ = new RMNCHCBACdetails();
		benCABCRMNCHROBJ.toString();

		Object[] elements = new Object[4];
		elements[0] = BigInteger.valueOf(987);
		elements[1] = "PNC";
		elements[2] = 2;
		elements[3] = 3;
		List<Object[]> objArraylist = new ArrayList<>();
		objArraylist.add(elements);

		List<Object> listOfObject = new ArrayList<>();
		listOfObject.add("Tuberculosis||Diabetes mellitus");
		;
		when(rMNCHBenDetailsRepo.getByIdAndVanID(m.getBenDetailsId(), a.getVanID())).thenReturn(benDetailsOBJ);
		when(rMNCHBenAccountRepo.getByIdAndVanID(m.getBenAccountID(), a.getVanID())).thenReturn(benAccountOBJ);
		when(rMNCHBenImageRepo.getByIdAndVanID(m.getBenImageId().longValue(), a.getVanID())).thenReturn(benImageOBJ);
		when(rMNCHBenAddressRepo.getByIdAndVanID(m.getBenAddressId(), a.getVanID())).thenReturn(benAddressOBJ);
		// when(rMNCHBenContactRepo.getByIdAndVanID(m.getBenContactsId(),
		// a.getVanID())).thenReturn(benContactOBJ);
		when(rMNCHMBenRegIdMapRepo.getBenIdFromRegID(m.getBenRegId().longValue())).thenReturn(BigInteger.valueOf(9));
		when(rMNCHBeneficiaryDetailsRmnchRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benDetailsRMNCHOBJ);
		when(rMNCHBornBirthDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benBotnBirthRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benCABCRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getVisitDetailsbyRegID(any())).thenReturn(objArraylist);

		when(rMNCHCBACDetailsRepo.getDiagnosisProvidedPNC(any(), any())).thenReturn(listOfObject);
		when(rMNCHMBenMappingRepo.getByAddressIDAndVanID(a.getId(), a.getVanID())).thenReturn(m);
		String resp = rmnchDataSyncServiceImpl.getBenData(json, null);
		JSONParser parser = new JSONParser();
		JSONObject jsonResp = (JSONObject) parser.parse(resp);
		String string = jsonResp.getAsString("totalPage");
		Integer valueOf = Integer.valueOf(string);
		Assertions.assertTrue(valueOf > 0);
	}

	@Test
	void testgetBenDataForANCCase() throws Exception {
		ReflectionTestUtils.setField(rmnchDataSyncServiceImpl, "door_to_door_page_size", "1");
		GetBenRequestHandler getBenRequestHandler = new GetBenRequestHandler();
		getBenRequestHandler.setAshaId(9);
		getBenRequestHandler.setVillageID(8);
		getBenRequestHandler.setPageNo(7);
		String json = new Gson().toJson(getBenRequestHandler);
		List<RMNCHMBeneficiaryaddress> list = new ArrayList<>();
		RMNCHMBeneficiaryaddress a = new RMNCHMBeneficiaryaddress();
		a.setCountyid(4);
		a.setId(BigInteger.valueOf(8));
		a.setVanID(8);
		list.add(a);
		Page<RMNCHMBeneficiaryaddress> pagermnch = new PageImpl<>(list);
		when(rMNCHBenAddressRepo.getBenData(any(), any())).thenReturn(pagermnch);

		RMNCHMBeneficiarymapping m = new RMNCHMBeneficiarymapping();
		m.setBenDetailsId(BigInteger.valueOf(9));
		m.setBenAccountID(BigInteger.valueOf(9));
		m.setBenImageId(BigInteger.valueOf(9));
		m.setBenAddressId(BigInteger.valueOf(9));
		m.setBenConsentId(BigInteger.valueOf(9));
		m.setBenRegId(BigInteger.valueOf(9));
		m.toString();
		RMNCHMBeneficiarydetail benDetailsOBJ = new RMNCHMBeneficiarydetail();
		benDetailsOBJ.toString();
		RMNCHMBeneficiaryAccount benAccountOBJ = new RMNCHMBeneficiaryAccount();
		benAccountOBJ.toString();
		RMNCHMBeneficiaryImage benImageOBJ = new RMNCHMBeneficiaryImage();
		benImageOBJ.toString();
		RMNCHMBeneficiaryaddress benAddressOBJ = new RMNCHMBeneficiaryaddress();
		benAddressOBJ.toString();
		RMNCHMBeneficiarycontact benContactOBJ = new RMNCHMBeneficiarycontact();
		RMNCHMBeneficiarycontact benContactOBJDuplicate = new RMNCHMBeneficiarycontact();
		benContactOBJ.equals(benContactOBJDuplicate);
		benContactOBJ.hashCode();
		benContactOBJ.toString();
		RMNCHBeneficiaryDetailsRmnch benDetailsRMNCHOBJ = new RMNCHBeneficiaryDetailsRmnch();
		benDetailsRMNCHOBJ.toString();

		RMNCHBornBirthDetails benBotnBirthRMNCHROBJ = new RMNCHBornBirthDetails();
		benBotnBirthRMNCHROBJ.toString();

		RMNCHCBACdetails benCABCRMNCHROBJ = new RMNCHCBACdetails();
		benCABCRMNCHROBJ.toString();

		Object[] elements = new Object[4];
		elements[0] = BigInteger.valueOf(987);
		elements[1] = "ANC";
		elements[2] = 2;
		elements[3] = 3;
		List<Object[]> objArraylist = new ArrayList<>();
		objArraylist.add(elements);

		List<Object> listOfObject = new ArrayList<>();
		listOfObject.add("Tuberculosis||Diabetes mellitus");
		;
		when(rMNCHBenDetailsRepo.getByIdAndVanID(m.getBenDetailsId(), a.getVanID())).thenReturn(benDetailsOBJ);
		when(rMNCHBenAccountRepo.getByIdAndVanID(m.getBenAccountID(), a.getVanID())).thenReturn(benAccountOBJ);
		when(rMNCHBenImageRepo.getByIdAndVanID(m.getBenImageId().longValue(), a.getVanID())).thenReturn(benImageOBJ);
		when(rMNCHBenAddressRepo.getByIdAndVanID(m.getBenAddressId(), a.getVanID())).thenReturn(benAddressOBJ);
		// when(rMNCHBenContactRepo.getByIdAndVanID(m.getBenContactsId(),
		// a.getVanID())).thenReturn(benContactOBJ);
		when(rMNCHMBenRegIdMapRepo.getBenIdFromRegID(m.getBenRegId().longValue())).thenReturn(BigInteger.valueOf(9));
		when(rMNCHBeneficiaryDetailsRmnchRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benDetailsRMNCHOBJ);
		when(rMNCHBornBirthDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benBotnBirthRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benCABCRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getVisitDetailsbyRegID(any())).thenReturn(objArraylist);

		when(rMNCHMBenMappingRepo.getByAddressIDAndVanID(a.getId(), a.getVanID())).thenReturn(m);
		String resp = rmnchDataSyncServiceImpl.getBenData(json, null);
		JSONParser parser = new JSONParser();
		JSONObject jsonResp = (JSONObject) parser.parse(resp);
		String string = jsonResp.getAsString("totalPage");
		Integer valueOf = Integer.valueOf(string);
		Assertions.assertTrue(valueOf > 0);

	}

	@Test
	void testgetBenDataForNCDcare() throws Exception {
		ReflectionTestUtils.setField(rmnchDataSyncServiceImpl, "door_to_door_page_size", "1");
		GetBenRequestHandler getBenRequestHandler = new GetBenRequestHandler();
		getBenRequestHandler.setAshaId(9);
		getBenRequestHandler.setVillageID(8);
		getBenRequestHandler.setPageNo(7);
		String json = new Gson().toJson(getBenRequestHandler);
		List<RMNCHMBeneficiaryaddress> list = new ArrayList<>();
		RMNCHMBeneficiaryaddress a = new RMNCHMBeneficiaryaddress();
		a.setCountyid(4);
		a.setId(BigInteger.valueOf(8));
		a.setVanID(8);
		list.add(a);
		Page<RMNCHMBeneficiaryaddress> pagermnch = new PageImpl<>(list);
		when(rMNCHBenAddressRepo.getBenData(any(), any())).thenReturn(pagermnch);

		RMNCHMBeneficiarymapping m = new RMNCHMBeneficiarymapping();
		m.setBenDetailsId(BigInteger.valueOf(9));
		m.setBenAccountID(BigInteger.valueOf(9));
		m.setBenImageId(BigInteger.valueOf(9));
		m.setBenAddressId(BigInteger.valueOf(9));
		m.setBenConsentId(BigInteger.valueOf(9));
		m.setBenRegId(BigInteger.valueOf(9));
		m.toString();
		RMNCHMBeneficiarydetail benDetailsOBJ = new RMNCHMBeneficiarydetail();

		benDetailsOBJ.setCommunity("Community");
		benDetailsOBJ.setCommunityId(5);
		benDetailsOBJ.setDob(null);
		benDetailsOBJ.setFirstName("R");
		benDetailsOBJ.setFatherName("Father");
		benDetailsOBJ.setGender("M");
		benDetailsOBJ.setGenderId(1);
		benDetailsOBJ.setMaritalstatus("Single");
		benDetailsOBJ.setMaritalstatusId(3);
		benDetailsOBJ.setMarriageDate(null);
		benDetailsOBJ.setReligion("H");
		benDetailsOBJ.setReligionID(BigInteger.valueOf(9));
		benDetailsOBJ.setSpousename("S");
		benDetailsOBJ.setDob(Timestamp.valueOf("2011-10-02 18:48:05.123"));
		benDetailsOBJ.setMarriageDate(Timestamp.valueOf("2011-10-02 18:48:05.123"));
		benDetailsOBJ.toString();
		RMNCHMBeneficiaryAccount benAccountOBJ = new RMNCHMBeneficiaryAccount();
		benAccountOBJ.toString();
		RMNCHMBeneficiaryImage benImageOBJ = new RMNCHMBeneficiaryImage();
		benImageOBJ.setUser_image("image");
		benImageOBJ.toString();
		RMNCHMBeneficiaryaddress benAddressOBJ = new RMNCHMBeneficiaryaddress();
		benAddressOBJ.toString();
		RMNCHMBeneficiarycontact benContactOBJ = new RMNCHMBeneficiarycontact();
		benContactOBJ.toString();
		RMNCHBeneficiaryDetailsRmnch benDetailsRMNCHOBJ = new RMNCHBeneficiaryDetailsRmnch();
		benDetailsRMNCHOBJ.toString();

		RMNCHBornBirthDetails benBotnBirthRMNCHROBJ = new RMNCHBornBirthDetails();
		benBotnBirthRMNCHROBJ.toString();

		RMNCHCBACdetails benCABCRMNCHROBJ = new RMNCHCBACdetails();
		RMNCHCBACdetails benCABCRMNCHROBJ2 = new RMNCHCBACdetails();
		benCABCRMNCHROBJ.equals(benCABCRMNCHROBJ2);
		benCABCRMNCHROBJ.hashCode();
		benCABCRMNCHROBJ.toString();

		Object[] elements = new Object[4];
		elements[0] = BigInteger.valueOf(987);
		elements[1] = "NCD care";
		elements[2] = 2;
		elements[3] = 3;
		List<Object[]> objArraylist = new ArrayList<>();
		objArraylist.add(elements);

		List<Object> listOfObject = new ArrayList<>();
		listOfObject.add("Tuberculosis||Diabetes mellitus");
		;
		when(rMNCHBenDetailsRepo.getByIdAndVanID(m.getBenDetailsId(), a.getVanID())).thenReturn(benDetailsOBJ);
		when(rMNCHBenAccountRepo.getByIdAndVanID(m.getBenAccountID(), a.getVanID())).thenReturn(benAccountOBJ);
		when(rMNCHBenImageRepo.getByIdAndVanID(m.getBenImageId().longValue(), a.getVanID())).thenReturn(benImageOBJ);
		when(rMNCHBenAddressRepo.getByIdAndVanID(m.getBenAddressId(), a.getVanID())).thenReturn(benAddressOBJ);
		// when(rMNCHBenContactRepo.getByIdAndVanID(m.getBenContactsId(),
		// a.getVanID())).thenReturn(benContactOBJ);
		when(rMNCHMBenRegIdMapRepo.getBenIdFromRegID(m.getBenRegId().longValue())).thenReturn(BigInteger.valueOf(9));
		when(rMNCHBeneficiaryDetailsRmnchRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benDetailsRMNCHOBJ);
		when(rMNCHBornBirthDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benBotnBirthRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benCABCRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getVisitDetailsbyRegID(any())).thenReturn(objArraylist);

		// when(rMNCHCBACDetailsRepo.getDiagnosisProvidedCommon(any(),
		// any())).thenReturn(listOfObject);

		when(rMNCHMBenMappingRepo.getByAddressIDAndVanID(a.getId(), a.getVanID())).thenReturn(m);
		String resp = rmnchDataSyncServiceImpl.getBenData(json, null);
		JSONParser parser = new JSONParser();
		JSONObject jsonResp = (JSONObject) parser.parse(resp);
		String string = jsonResp.getAsString("totalPage");
		Integer valueOf = Integer.valueOf(string);
		Assertions.assertTrue(valueOf > 0);
	}

	@Test
	void testgetBenDataForNCDscreening() throws Exception {
		ReflectionTestUtils.setField(rmnchDataSyncServiceImpl, "door_to_door_page_size", "1");
		GetBenRequestHandler getBenRequestHandler = new GetBenRequestHandler();
		getBenRequestHandler.setAshaId(9);
		getBenRequestHandler.setVillageID(8);
		getBenRequestHandler.setPageNo(7);
		String json = new Gson().toJson(getBenRequestHandler);
		List<RMNCHMBeneficiaryaddress> list = new ArrayList<>();
		RMNCHMBeneficiaryaddress a = new RMNCHMBeneficiaryaddress();
		a.setCountyid(4);
		a.setId(BigInteger.valueOf(8));
		a.setVanID(8);
		list.add(a);
		Page<RMNCHMBeneficiaryaddress> pagermnch = new PageImpl<>(list);
		when(rMNCHBenAddressRepo.getBenData(any(), any())).thenReturn(pagermnch);

		RMNCHMBeneficiarymapping m = new RMNCHMBeneficiarymapping();
		m.setBenDetailsId(BigInteger.valueOf(9));
		m.setBenAccountID(BigInteger.valueOf(9));
		m.setBenImageId(BigInteger.valueOf(9));
		m.setBenAddressId(BigInteger.valueOf(9));
		m.setBenConsentId(BigInteger.valueOf(9));
		m.setBenRegId(BigInteger.valueOf(9));
		m.toString();
		RMNCHMBeneficiarydetail benDetailsOBJ = new RMNCHMBeneficiarydetail();

		benDetailsOBJ.setCommunity("Community");
		benDetailsOBJ.setCommunityId(5);
		benDetailsOBJ.setDob(null);
		benDetailsOBJ.setFirstName("R");
		benDetailsOBJ.setFatherName("Father");
		benDetailsOBJ.setGender("M");
		benDetailsOBJ.setGenderId(1);
		benDetailsOBJ.setMaritalstatus("Single");
		benDetailsOBJ.setMaritalstatusId(3);
		benDetailsOBJ.setMarriageDate(null);
		benDetailsOBJ.setReligion("H");
		benDetailsOBJ.setReligionID(BigInteger.valueOf(9));
		benDetailsOBJ.setSpousename("S");
		benDetailsOBJ.setDob(Timestamp.valueOf("2011-10-02 18:48:05.123"));
		benDetailsOBJ.setMarriageDate(Timestamp.valueOf("2011-10-02 18:48:05.123"));
		benDetailsOBJ.toString();
		RMNCHMBeneficiaryAccount benAccountOBJ = new RMNCHMBeneficiaryAccount();
		benAccountOBJ.toString();
		RMNCHMBeneficiaryImage benImageOBJ = new RMNCHMBeneficiaryImage();
		benImageOBJ.setUser_image("image");
		benImageOBJ.toString();
		RMNCHMBeneficiaryaddress benAddressOBJ = new RMNCHMBeneficiaryaddress();
		benAddressOBJ.toString();
		RMNCHMBeneficiarycontact benContactOBJ = new RMNCHMBeneficiarycontact();
		benContactOBJ.toString();
		RMNCHBeneficiaryDetailsRmnch benDetailsRMNCHOBJ = new RMNCHBeneficiaryDetailsRmnch();
		benDetailsRMNCHOBJ.toString();

		RMNCHBornBirthDetails benBotnBirthRMNCHROBJ = new RMNCHBornBirthDetails();
		benBotnBirthRMNCHROBJ.toString();

		RMNCHCBACdetails benCABCRMNCHROBJ = new RMNCHCBACdetails();
		benCABCRMNCHROBJ.toString();

		Object[] elements = new Object[4];
		elements[0] = BigInteger.valueOf(987);
		elements[1] = "NCD screening";
		elements[2] = 2;
		elements[3] = 3;
		List<Object[]> objArraylist = new ArrayList<>();
		objArraylist.add(elements);

		List<Object> listOfObject = new ArrayList<>();
		listOfObject.add("Tuberculosis||Diabetes mellitus");
		;
		when(rMNCHBenDetailsRepo.getByIdAndVanID(m.getBenDetailsId(), a.getVanID())).thenReturn(benDetailsOBJ);
		when(rMNCHBenAccountRepo.getByIdAndVanID(m.getBenAccountID(), a.getVanID())).thenReturn(benAccountOBJ);
		when(rMNCHBenImageRepo.getByIdAndVanID(m.getBenImageId().longValue(), a.getVanID())).thenReturn(benImageOBJ);
		when(rMNCHBenAddressRepo.getByIdAndVanID(m.getBenAddressId(), a.getVanID())).thenReturn(benAddressOBJ);
		// when(rMNCHBenContactRepo.getByIdAndVanID(m.getBenContactsId(),
		// a.getVanID())).thenReturn(benContactOBJ);
		when(rMNCHMBenRegIdMapRepo.getBenIdFromRegID(m.getBenRegId().longValue())).thenReturn(BigInteger.valueOf(9));
		when(rMNCHBeneficiaryDetailsRmnchRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benDetailsRMNCHOBJ);
		when(rMNCHBornBirthDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benBotnBirthRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benCABCRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getVisitDetailsbyRegID(any())).thenReturn(objArraylist);

		when(rMNCHCBACDetailsRepo.getDiagnosisProvidedCommon(any(), any())).thenReturn(listOfObject);

		when(rMNCHMBenMappingRepo.getByAddressIDAndVanID(a.getId(), a.getVanID())).thenReturn(m);
		String resp = rmnchDataSyncServiceImpl.getBenData(json, null);
		JSONParser parser = new JSONParser();
		JSONObject jsonResp = (JSONObject) parser.parse(resp);
		String string = jsonResp.getAsString("totalPage");
		Integer valueOf = Integer.valueOf(string);
		Assertions.assertTrue(valueOf > 0);
	}

	@Test
	void testgetBenDataForCOVID19Screening() throws Exception {
		ReflectionTestUtils.setField(rmnchDataSyncServiceImpl, "door_to_door_page_size", "1");
		GetBenRequestHandler getBenRequestHandler = new GetBenRequestHandler();
		getBenRequestHandler.setAshaId(9);
		getBenRequestHandler.setVillageID(8);
		getBenRequestHandler.setPageNo(7);
		String json = new Gson().toJson(getBenRequestHandler);
		List<RMNCHMBeneficiaryaddress> list = new ArrayList<>();
		RMNCHMBeneficiaryaddress a = new RMNCHMBeneficiaryaddress();
		a.setCountyid(4);
		a.setId(BigInteger.valueOf(8));
		a.setVanID(8);
		list.add(a);
		Page<RMNCHMBeneficiaryaddress> pagermnch = new PageImpl<>(list);
		when(rMNCHBenAddressRepo.getBenData(any(), any())).thenReturn(pagermnch);

		RMNCHMBeneficiarymapping m = new RMNCHMBeneficiarymapping();
		m.setBenDetailsId(BigInteger.valueOf(9));
		m.setBenAccountID(BigInteger.valueOf(9));
		m.setBenImageId(BigInteger.valueOf(9));
		m.setBenAddressId(BigInteger.valueOf(9));
		m.setBenConsentId(BigInteger.valueOf(9));
		m.setBenRegId(BigInteger.valueOf(9));
		m.toString();
		RMNCHMBeneficiarydetail benDetailsOBJ = new RMNCHMBeneficiarydetail();

		benDetailsOBJ.setCommunity("Community");
		benDetailsOBJ.setCommunityId(5);
		benDetailsOBJ.setDob(null);
		benDetailsOBJ.setFirstName("R");
		benDetailsOBJ.setFatherName("Father");
		benDetailsOBJ.setGender("M");
		benDetailsOBJ.setGenderId(1);
		benDetailsOBJ.setMaritalstatus("Single");
		benDetailsOBJ.setMaritalstatusId(3);
		benDetailsOBJ.setMarriageDate(null);
		benDetailsOBJ.setReligion("H");
		benDetailsOBJ.setReligionID(BigInteger.valueOf(9));
		benDetailsOBJ.setSpousename("S");
		benDetailsOBJ.setDob(Timestamp.valueOf("2011-10-02 18:48:05.123"));
		benDetailsOBJ.setMarriageDate(Timestamp.valueOf("2011-10-02 18:48:05.123"));
		benDetailsOBJ.toString();
		RMNCHMBeneficiaryAccount benAccountOBJ = new RMNCHMBeneficiaryAccount();
		benAccountOBJ.toString();
		RMNCHMBeneficiaryImage benImageOBJ = new RMNCHMBeneficiaryImage();
		benImageOBJ.setUser_image("image");
		benImageOBJ.toString();
		RMNCHMBeneficiaryaddress benAddressOBJ = new RMNCHMBeneficiaryaddress();
		benAddressOBJ.toString();
		RMNCHMBeneficiarycontact benContactOBJ = new RMNCHMBeneficiarycontact();
		benContactOBJ.toString();
		RMNCHBeneficiaryDetailsRmnch benDetailsRMNCHOBJ = new RMNCHBeneficiaryDetailsRmnch();
		benDetailsRMNCHOBJ.toString();

		RMNCHBornBirthDetails benBotnBirthRMNCHROBJ = new RMNCHBornBirthDetails();
		benBotnBirthRMNCHROBJ.toString();

		RMNCHCBACdetails benCABCRMNCHROBJ = new RMNCHCBACdetails();
		benCABCRMNCHROBJ.toString();

		Object[] elements = new Object[4];
		elements[0] = BigInteger.valueOf(987);
		elements[1] = "COVID-19 Screening";
		elements[2] = 2;
		elements[3] = 3;
		List<Object[]> objArraylist = new ArrayList<>();
		objArraylist.add(elements);

		List<Object> listOfObject = new ArrayList<>();
		listOfObject.add("Tuberculosis||Diabetes mellitus");
		;
		when(rMNCHBenDetailsRepo.getByIdAndVanID(m.getBenDetailsId(), a.getVanID())).thenReturn(benDetailsOBJ);
		when(rMNCHBenAccountRepo.getByIdAndVanID(m.getBenAccountID(), a.getVanID())).thenReturn(benAccountOBJ);
		when(rMNCHBenImageRepo.getByIdAndVanID(m.getBenImageId().longValue(), a.getVanID())).thenReturn(benImageOBJ);
		when(rMNCHBenAddressRepo.getByIdAndVanID(m.getBenAddressId(), a.getVanID())).thenReturn(benAddressOBJ);
		// when(rMNCHBenContactRepo.getByIdAndVanID(m.getBenContactsId(),
		// a.getVanID())).thenReturn(benContactOBJ);
		when(rMNCHMBenRegIdMapRepo.getBenIdFromRegID(m.getBenRegId().longValue())).thenReturn(BigInteger.valueOf(9));
		when(rMNCHBeneficiaryDetailsRmnchRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benDetailsRMNCHOBJ);
		when(rMNCHBornBirthDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benBotnBirthRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getByRegID((m.getBenRegId()).longValue())).thenReturn(benCABCRMNCHROBJ);
		when(rMNCHCBACDetailsRepo.getVisitDetailsbyRegID(any())).thenReturn(objArraylist);

		when(rMNCHCBACDetailsRepo.getDiagnosisProvidedCommon(any(), any())).thenReturn(listOfObject);

		when(rMNCHMBenMappingRepo.getByAddressIDAndVanID(a.getId(), a.getVanID())).thenReturn(m);
		String resp = rmnchDataSyncServiceImpl.getBenData(json, null);
		JSONParser parser = new JSONParser();
		JSONObject jsonResp = (JSONObject) parser.parse(resp);
		String string = jsonResp.getAsString("totalPage");
		Integer valueOf = Integer.valueOf(string);
		Assertions.assertTrue(valueOf > 0);
	}

	private String getData(String resp, String status) throws ParseException {
		JSONParser parser = new JSONParser();
		JSONObject json = (JSONObject) parser.parse(resp);
		JSONObject object = (JSONObject) json.get("response");
		String actualresp = object.getAsString(status);
		return actualresp;
	}
}
