package com.iemr.common.identity.service.rmnch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.iemr.common.identity.data.rmnch.RMNCHBeneficiaryDetailsRmnch;
import com.iemr.common.identity.data.rmnch.RMNCHBornBirthDetails;
import com.iemr.common.identity.data.rmnch.RMNCHCBACdetails;
import com.iemr.common.identity.data.rmnch.RMNCHHouseHoldDetails;
import com.iemr.common.identity.repo.rmnch.RMNCHBenDetailsRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBeneficiaryDetailsRmnchRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHBornBirthDetailsRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHCBACDetailsRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHHouseHoldDetailsRepo;
import com.iemr.common.identity.repo.rmnch.RMNCHMBenRegIdMapRepo;

/**
 * STOP-750: the tablet sends its own sync status in "Processed" ("P" once it has pushed a record
 * before). Records received in syncDataToAmrit must always be saved as pending upload ("N"),
 * otherwise the van's MMU sync never uploads them to central.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RmnchDataSyncServiceImplProcessedTest {

	@Mock
	private RMNCHBeneficiaryDetailsRmnchRepo rMNCHBeneficiaryDetailsRmnchRepo;
	@Mock
	private RMNCHBornBirthDetailsRepo rMNCHBornBirthDetailsRepo;
	@Mock
	private RMNCHCBACDetailsRepo rMNCHCBACDetailsRepo;
	@Mock
	private RMNCHHouseHoldDetailsRepo rMNCHHouseHoldDetailsRepo;
	@Mock
	private RMNCHBenDetailsRepo rMNCHBenDetailsRepo;
	@Mock
	private RMNCHMBenRegIdMapRepo rMNCHMBenRegIdMapRepo;

	@InjectMocks
	private RmnchDataSyncServiceImpl service;

	@BeforeEach
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void setUp() {
		when(rMNCHMBenRegIdMapRepo.getRegID(any())).thenReturn(BigInteger.valueOf(31495L));
		when(rMNCHBeneficiaryDetailsRmnchRepo.saveAll(any()))
				.thenAnswer(inv -> new ArrayList<>((Collection) inv.getArgument(0)));
		when(rMNCHBornBirthDetailsRepo.saveAll(any()))
				.thenAnswer(inv -> new ArrayList<>((Collection) inv.getArgument(0)));
		when(rMNCHCBACDetailsRepo.saveAll(any()))
				.thenAnswer(inv -> new ArrayList<>((Collection) inv.getArgument(0)));
		when(rMNCHHouseHoldDetailsRepo.saveAll(any()))
				.thenAnswer(inv -> new ArrayList<>((Collection) inv.getArgument(0)));
	}

	private static String request(String processed) {
		return "{"
				+ "\"beneficiaryDetails\":[{\"benficieryid\":290611419892,\"houseoldId\":1795505567251,"
				+ "\"Processed\":\"" + processed + "\"}],"
				+ "\"bornBirthDeatils\":[{\"benficieryid\":290611419892,\"Processed\":\"" + processed + "\"}],"
				+ "\"cBACDetails\":[{\"benficieryid\":290611419892,\"Processed\":\"" + processed + "\"}],"
				+ "\"houseHoldDetails\":[{\"houseoldId\":1795505567251,\"Processed\":\"" + processed + "\"}]"
				+ "}";
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <T> List<T> saved(ArgumentCaptor<Iterable> captor) {
		List<T> out = new ArrayList<>();
		((Iterable<T>) captor.getValue()).forEach(out::add);
		return out;
	}

	@Test
	@SuppressWarnings({ "rawtypes" })
	void householdResentWithTabletStatusPIsSavedAsPendingUpload() throws Exception {
		service.syncDataToAmrit(request("P"), "jwt");

		ArgumentCaptor<Iterable> captor = ArgumentCaptor.forClass(Iterable.class);
		verify(rMNCHHouseHoldDetailsRepo).saveAll(captor.capture());
		List<RMNCHHouseHoldDetails> households = saved(captor);
		assertEquals(1, households.size());
		assertEquals("N", households.get(0).getProcessed());
	}

	@Test
	@SuppressWarnings({ "rawtypes" })
	void existingHouseholdResentWithPIsSavedAsPendingUpload() throws Exception {
		RMNCHHouseHoldDetails stored = new RMNCHHouseHoldDetails();
		stored.setHouseHoldDetailsId(5629L);
		stored.setProcessed("P");
		when(rMNCHHouseHoldDetailsRepo.getByHouseHoldID(anyLong())).thenReturn(List.of(stored));

		service.syncDataToAmrit(request("P"), "jwt");

		ArgumentCaptor<Iterable> captor = ArgumentCaptor.forClass(Iterable.class);
		verify(rMNCHHouseHoldDetailsRepo).saveAll(captor.capture());
		List<RMNCHHouseHoldDetails> households = saved(captor);
		assertEquals(Long.valueOf(5629L), households.get(0).getHouseHoldDetailsId());
		assertEquals("N", households.get(0).getProcessed());
	}

	@Test
	@SuppressWarnings({ "rawtypes" })
	void beneficiaryBirthAndCbacResentWithPAreSavedAsPendingUpload() throws Exception {
		service.syncDataToAmrit(request("P"), "jwt");

		ArgumentCaptor<Iterable> benCaptor = ArgumentCaptor.forClass(Iterable.class);
		verify(rMNCHBeneficiaryDetailsRmnchRepo).saveAll(benCaptor.capture());
		List<RMNCHBeneficiaryDetailsRmnch> bens = saved(benCaptor);
		assertEquals("N", bens.get(0).getProcessed());

		ArgumentCaptor<Iterable> birthCaptor = ArgumentCaptor.forClass(Iterable.class);
		verify(rMNCHBornBirthDetailsRepo).saveAll(birthCaptor.capture());
		List<RMNCHBornBirthDetails> births = saved(birthCaptor);
		assertEquals("N", births.get(0).getProcessed());

		ArgumentCaptor<Iterable> cbacCaptor = ArgumentCaptor.forClass(Iterable.class);
		verify(rMNCHCBACDetailsRepo).saveAll(cbacCaptor.capture());
		List<RMNCHCBACdetails> cbacs = saved(cbacCaptor);
		assertEquals("N", cbacs.get(0).getProcessed());
	}

	@Test
	@SuppressWarnings({ "rawtypes" })
	void householdSentAsNewOrEditedStaysPendingUpload() throws Exception {
		for (String status : new String[] { "N", "U" }) {
			org.mockito.Mockito.clearInvocations(rMNCHHouseHoldDetailsRepo);
			service.syncDataToAmrit(request(status), "jwt");

			ArgumentCaptor<Iterable> captor = ArgumentCaptor.forClass(Iterable.class);
			verify(rMNCHHouseHoldDetailsRepo).saveAll(captor.capture());
			List<RMNCHHouseHoldDetails> households = saved(captor);
			assertEquals("N", households.get(0).getProcessed(), "tablet status " + status);
		}
	}
}
