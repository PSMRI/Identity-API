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
package com.iemr.common.identity.service.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigInteger;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.util.ObjectBuilder;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;

/**
 * Tests for the per-beneficiary index update triggered after a create or edit.
 *
 * <p>
 * This runs asynchronously off the request thread, so a failure here can never
 * be allowed to surface to the caller - a beneficiary must stay registered even
 * if the index is down. The other thing worth pinning is the document id: it
 * has to be the beneficiary id, matching the bulk sync and the delete path.
 * Keying it on the registration id instead wrote a second document per
 * beneficiary, so edits never appeared in search.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BeneficiaryElasticsearchIndexUpdaterTest {

	@Mock
	private ElasticsearchClient esClient;
	@Mock
	private BeneficiaryDocumentDataService dataService;

	@InjectMocks
	private BeneficiaryElasticsearchIndexUpdater updater;

	private static final BigInteger BEN_REG_ID = BigInteger.valueOf(100200300L);

	private IndexRequest<?> capturedIndexRequest;

	@BeforeEach
	@SuppressWarnings({ "unchecked", "rawtypes" })
	void configureUpdater() throws IOException {
		ReflectionTestUtils.setField(updater, "beneficiaryIndex", "beneficiary");
		ReflectionTestUtils.setField(updater, "esEnabled", true);
		capturedIndexRequest = null;
		when(esClient.index(any(Function.class))).thenAnswer(invocation -> {
			Function<IndexRequest.Builder, ObjectBuilder<IndexRequest>> builder = invocation.getArgument(0);
			capturedIndexRequest = builder.apply(new IndexRequest.Builder<>()).build();
			return IndexResponse.of(r -> r.index("beneficiary").id("4001").version(1L).result(Result.Updated)
					.seqNo(1L).primaryTerm(1L).shards(s -> s.total(1).successful(1).failed(0)));
		});
	}

	private BeneficiaryDocument document(String benId) {
		BeneficiaryDocument document = new BeneficiaryDocument();
		document.setBenId(benId);
		document.setBenRegId(100200300L);
		document.setFirstName("Asha");
		return document;
	}

	@Test
	@DisplayName("an edited beneficiary is re-indexed under its beneficiary id")
	void editedBeneficiaryIsReindexedUnderItsBeneficiaryId() throws Exception {
		when(dataService.getBeneficiaryWithAbhaDetails(BEN_REG_ID)).thenReturn(document("4001"));

		assertTrue(updater.syncBeneficiaryAsync(BEN_REG_ID).isDone());

		assertNotNull(capturedIndexRequest);
		assertEquals("beneficiary", capturedIndexRequest.index());
		assertEquals("4001", capturedIndexRequest.id());
	}

	@Test
	@DisplayName("the re-index refreshes immediately so the edit is searchable at once")
	void reindexRefreshesImmediately() throws Exception {
		// The agent who just saved the edit searches again straight away.
		when(dataService.getBeneficiaryWithAbhaDetails(BEN_REG_ID)).thenReturn(document("4001"));

		updater.syncBeneficiaryAsync(BEN_REG_ID).get();

		assertEquals(co.elastic.clients.elasticsearch._types.Refresh.True, capturedIndexRequest.refresh());
	}

	@Test
	@DisplayName("a beneficiary with no document is skipped")
	void beneficiaryWithNoDocumentIsSkipped() throws Exception {
		when(dataService.getBeneficiaryWithAbhaDetails(BEN_REG_ID)).thenReturn(null);

		updater.syncBeneficiaryAsync(BEN_REG_ID).get();

		verifyNoInteractions(esClient);
	}

	@Test
	@DisplayName("a document with no beneficiary id is skipped rather than indexed under a guessed id")
	void documentWithNoBeneficiaryIdIsSkipped() throws Exception {
		when(dataService.getBeneficiaryWithAbhaDetails(BEN_REG_ID)).thenReturn(document(null));

		updater.syncBeneficiaryAsync(BEN_REG_ID).get();

		verifyNoInteractions(esClient);
	}

	@Test
	@DisplayName("an index failure does not fail the edit that triggered it")
	void indexFailureDoesNotFailTheEdit() throws Exception {
		when(dataService.getBeneficiaryWithAbhaDetails(BEN_REG_ID)).thenReturn(document("4001"));
		when(esClient.index(any(Function.class))).thenThrow(new IOException("index unavailable"));

		assertTrue(updater.syncBeneficiaryAsync(BEN_REG_ID).isDone());
	}

	@Test
	@DisplayName("a deleted beneficiary is removed from the index under its beneficiary id")
	void deletedBeneficiaryIsRemovedFromTheIndex() throws Exception {
		when(esClient.delete(any(DeleteRequest.class))).thenReturn(DeleteResponse.of(r -> r.index("beneficiary")
				.id("4001").version(1L).result(Result.Deleted).seqNo(1L).primaryTerm(1L)
				.shards(s -> s.total(1).successful(1).failed(0))));

		updater.deleteBeneficiaryAsync("4001");

		ArgumentCaptor<DeleteRequest> captor = ArgumentCaptor.forClass(DeleteRequest.class);
		verify(esClient).delete(captor.capture());
		assertEquals("beneficiary", captor.getValue().index());
		assertEquals("4001", captor.getValue().id());
	}

	@Test
	@DisplayName("nothing is sent to the index when the integration is switched off")
	void nothingIsSentWhenIntegrationIsOff() throws Exception {
		ReflectionTestUtils.setField(updater, "esEnabled", false);

		updater.deleteBeneficiaryAsync("4001");

		verify(esClient, never()).delete(any(DeleteRequest.class));
	}

	@Test
	@DisplayName("a delete failure is swallowed rather than surfaced to the caller")
	void deleteFailureIsSwallowed() throws Exception {
		when(esClient.delete(any(DeleteRequest.class))).thenThrow(new IOException("index unavailable"));

		updater.deleteBeneficiaryAsync("4001");

		verify(esClient).delete(any(DeleteRequest.class));
	}

	private static void assertEquals(Object expected, Object actual) {
		org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
	}
}
