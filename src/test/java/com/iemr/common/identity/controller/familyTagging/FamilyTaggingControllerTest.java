package com.iemr.common.identity.controller.familyTagging;

import static com.iemr.common.identity.TestJson.assertFailure;
import static com.iemr.common.identity.TestJson.assertSuccess;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.iemr.common.identity.service.familyTagging.FamilyTagService;

@ExtendWith(MockitoExtension.class)
class FamilyTaggingControllerTest {

	@Mock
	private FamilyTagService familyTagService;

	@InjectMocks
	private FamilyTaggingController controller;

	private static final String REQUEST = "{\"benId\":1}";

	@Test
	@DisplayName("saveFamilyTagging returns service payload on success")
	void saveFamilyTaggingSuccess() throws Exception {
		when(familyTagService.addTag(REQUEST)).thenReturn("{\"familyId\":\"F1\"}");

		assertSuccess(controller.saveFamilyTagging(REQUEST), "F1");
		verify(familyTagService).addTag(REQUEST);
	}

	@Test
	@DisplayName("saveFamilyTagging wraps service failure")
	void saveFamilyTaggingFailure() throws Exception {
		when(familyTagService.addTag(anyString())).thenThrow(new RuntimeException("tag failed"));

		assertFailure(controller.saveFamilyTagging(REQUEST), "Error in saving family tagging");
	}

	@Test
	@DisplayName("createFamily returns service payload on success")
	void createFamilySuccess() throws Exception {
		when(familyTagService.createFamily(REQUEST)).thenReturn("{\"familyId\":\"F2\"}");

		assertSuccess(controller.createFamily(REQUEST), "F2");
	}

	@Test
	@DisplayName("createFamily wraps service failure")
	void createFamilyFailure() throws Exception {
		when(familyTagService.createFamily(anyString())).thenThrow(new RuntimeException("create failed"));

		assertFailure(controller.createFamily(REQUEST), "Error in saving family tagging");
	}

	@Test
	@DisplayName("searchFamily returns service payload on success")
	void searchFamilySuccess() throws Exception {
		when(familyTagService.searchFamily(REQUEST)).thenReturn("[{\"familyId\":\"F3\"}]");

		assertSuccess(controller.searchFamily(REQUEST), "F3");
	}

	@Test
	@DisplayName("searchFamily wraps service failure")
	void searchFamilyFailure() throws Exception {
		when(familyTagService.searchFamily(anyString())).thenThrow(new RuntimeException("search failed"));

		assertFailure(controller.searchFamily(REQUEST), "Error in searching family");
	}

	@Test
	@DisplayName("getFamilyDatails returns service payload on success")
	void getFamilyDetailsSuccess() throws Exception {
		when(familyTagService.getFamilyDetails(REQUEST)).thenReturn("[{\"benId\":9}]");

		assertSuccess(controller.getFamilyDatails(REQUEST), "benId");
	}

	@Test
	@DisplayName("getFamilyDatails wraps service failure")
	void getFamilyDetailsFailure() throws Exception {
		when(familyTagService.getFamilyDetails(anyString())).thenThrow(new RuntimeException("details failed"));

		assertFailure(controller.getFamilyDatails(REQUEST), "Error in searching family members");
	}

	@Test
	@DisplayName("untagFamily returns service payload on success")
	void untagFamilySuccess() throws Exception {
		when(familyTagService.doFamilyUntag(REQUEST)).thenReturn("untagged");

		assertSuccess(controller.untagFamily(REQUEST), "untagged");
	}

	@Test
	@DisplayName("untagFamily wraps service failure")
	void untagFamilyFailure() throws Exception {
		when(familyTagService.doFamilyUntag(anyString())).thenThrow(new RuntimeException("untag failed"));

		assertFailure(controller.untagFamily(REQUEST), "Error in untagging family");
	}

	@Test
	@DisplayName("getFamilyDetailsByBeneficiaryId returns service payload on success")
	void getFamilyDetailsByBeneficiaryIdSuccess() throws Exception {
		when(familyTagService.getFamilyDetailsByBeneficiaryId(REQUEST)).thenReturn("{\"familyId\":\"F4\"}");

		assertSuccess(controller.getFamilyDetailsByBeneficiaryId(REQUEST), "F4");
	}

	@Test
	@DisplayName("getFamilyDetailsByBeneficiaryId wraps service failure")
	void getFamilyDetailsByBeneficiaryIdFailure() throws Exception {
		when(familyTagService.getFamilyDetailsByBeneficiaryId(anyString()))
				.thenThrow(new RuntimeException("lookup failed"));

		assertFailure(controller.getFamilyDetailsByBeneficiaryId(REQUEST),
				"Error in fetching family details by beneficiary ID");
	}

	@Test
	@DisplayName("editFamilyDetails returns service payload on success")
	void editFamilyDetailsSuccess() throws Exception {
		when(familyTagService.editFamilyDetails(REQUEST)).thenReturn("edited");

		assertSuccess(controller.editFamilyDetails(REQUEST), "edited");
	}

	@Test
	@DisplayName("editFamilyDetails wraps service failure")
	void editFamilyDetailsFailure() throws Exception {
		when(familyTagService.editFamilyDetails(anyString())).thenThrow(new RuntimeException("edit failed"));

		assertFailure(controller.editFamilyDetails(REQUEST), "Error in editing family details");
	}
}
