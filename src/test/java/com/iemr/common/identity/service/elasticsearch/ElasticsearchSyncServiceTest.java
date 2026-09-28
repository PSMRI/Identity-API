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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
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
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.util.ObjectBuilder;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;

/**
 * Tests for the full-index rebuild that backs the beneficiary search.
 *
 * <p>
 * The rebuild walks ~800k beneficiaries in database pages, batch-loads each
 * page, and bulk-indexes in fixed-size chunks. What the tests protect is the
 * paging and accounting around that loop: the run must stop when a page comes
 * back empty rather than looping to the declared total, a bulk request that
 * partially fails must be counted per document rather than all-or-nothing, and
 * a mid-run failure must be reported in the result instead of thrown, because
 * the sync is triggered from an endpoint that reports progress.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ElasticsearchSyncServiceTest {

	@Mock
	private ElasticsearchClient esClient;
	@Mock
	private BeneficiaryTransactionHelper transactionalWrapper;
	@Mock
	private BeneficiaryDocumentDataService documentDataService;

	@InjectMocks
	private ElasticsearchSyncService service;

	/** The request the bulk mock received, for assertions on what was indexed. */
	private BulkRequest capturedBulkRequest;

	@BeforeEach
	void configureIndexName() {
		ReflectionTestUtils.setField(service, "beneficiaryIndex", "beneficiary");
		capturedBulkRequest = null;
	}

	private BeneficiaryDocument document(String benId) {
		BeneficiaryDocument document = new BeneficiaryDocument();
		document.setBenId(benId);
		document.setFirstName("Asha");
		return document;
	}

	private BeneficiaryDocument documentWithAbha(String benId) {
		BeneficiaryDocument document = document(benId);
		document.setHealthID("asha@abdm");
		document.setAbhaID("12-3456-7890-1234");
		return document;
	}

	/** Stubs a bulk call that succeeds for every operation in the request. */
	private void stubBulkSuccess() throws IOException {
		when(esClient.bulk(any(BulkRequest.class))).thenAnswer(invocation -> {
			capturedBulkRequest = invocation.getArgument(0);
			return BulkResponse.of(b -> b.took(1).errors(false).items(Collections.emptyList()));
		});
	}

	/**
	 * Stubs a bulk call where the first {@code failures} operations are rejected
	 * and the rest succeed.
	 */
	private void stubBulkPartialFailure(int failures) throws IOException {
		when(esClient.bulk(any(BulkRequest.class))).thenAnswer(invocation -> {
			BulkRequest request = invocation.getArgument(0);
			capturedBulkRequest = request;
			List<BulkResponseItem> items = new ArrayList<>();
			for (int i = 0; i < request.operations().size(); i++) {
				final boolean failed = i < failures;
				final String id = String.valueOf(i);
				items.add(BulkResponseItem.of(item -> {
					item.index("beneficiary").id(id).status(failed ? 400 : 201)
							.operationType(co.elastic.clients.elasticsearch.core.bulk.OperationType.Index);
					if (failed) {
						item.error(ErrorCause.of(e -> e.type("mapper_parsing_exception").reason("bad field")));
					}
					return item;
				}));
			}
			return BulkResponse.of(b -> b.took(1).errors(failures > 0).items(items));
		});
	}

	/** Stubs one page of beneficiary IDs followed by an empty page. */
	private void stubOnePageOfIds(int count) {
		List<Object[]> page = IntStream.range(0, count)
				.mapToObj(index -> new Object[] { BigInteger.valueOf(1000L + index) })
				.collect(java.util.stream.Collectors.toList());
		when(transactionalWrapper.getBeneficiaryIdsBatch(eq(0), anyInt())).thenReturn(page);
		when(transactionalWrapper.getBeneficiaryIdsBatch(eq(10000), anyInt())).thenReturn(Collections.emptyList());
	}

	@Nested
	@DisplayName("full rebuild")
	class FullRebuild {

		@Test
		@DisplayName("an empty database is not indexed at all")
		void emptyDatabaseIsNotIndexed() throws Exception {
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(0L);

			ElasticsearchSyncService.SyncResult result = service.syncAllBeneficiaries();

			assertEquals(0, result.getSuccessCount());
			assertEquals(0, result.getFailureCount());
			verify(transactionalWrapper, never()).getBeneficiaryIdsBatch(anyInt(), anyInt());
		}

		@Test
		@DisplayName("every loaded beneficiary is indexed and counted")
		void everyLoadedBeneficiaryIsIndexedAndCounted() throws Exception {
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(3L);
			stubOnePageOfIds(3);
			when(documentDataService.getBeneficiariesBatch(any()))
					.thenReturn(Arrays.asList(document("1"), document("2"), document("3")));
			stubBulkSuccess();

			ElasticsearchSyncService.SyncResult result = service.syncAllBeneficiaries();

			assertEquals(3, result.getSuccessCount());
			assertEquals(0, result.getFailureCount());
			assertEquals(3, capturedBulkRequest.operations().size());
		}

		@Test
		@DisplayName("documents are indexed into the configured index under their beneficiary id")
		void documentsAreIndexedUnderTheirBeneficiaryId() throws Exception {
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(1L);
			stubOnePageOfIds(1);
			when(documentDataService.getBeneficiariesBatch(any()))
					.thenReturn(Collections.singletonList(document("4001")));
			stubBulkSuccess();

			service.syncAllBeneficiaries();

			assertEquals("beneficiary", capturedBulkRequest.operations().get(0).index().index());
			assertEquals("4001", capturedBulkRequest.operations().get(0).index().id());
		}

		@Test
		@DisplayName("a document with no beneficiary id is counted as a failure, not indexed")
		void documentWithNoIdIsCountedAsFailure() throws Exception {
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(2L);
			stubOnePageOfIds(2);
			when(documentDataService.getBeneficiariesBatch(any()))
					.thenReturn(Arrays.asList(document("1"), document(null), null));
			stubBulkSuccess();

			ElasticsearchSyncService.SyncResult result = service.syncAllBeneficiaries();

			assertEquals(1, result.getSuccessCount());
			assertEquals(2, result.getFailureCount());
		}

		@Test
		@DisplayName("a partially rejected bulk request is accounted for per document")
		void partiallyRejectedBulkIsAccountedPerDocument() throws Exception {
			// Counting the whole batch as failed would hide that most of it
			// landed, and re-running the sync is the only remedy.
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(3L);
			stubOnePageOfIds(3);
			when(documentDataService.getBeneficiariesBatch(any()))
					.thenReturn(Arrays.asList(document("1"), document("2"), document("3")));
			stubBulkPartialFailure(1);

			ElasticsearchSyncService.SyncResult result = service.syncAllBeneficiaries();

			assertEquals(2, result.getSuccessCount());
			assertEquals(1, result.getFailureCount());
		}

		@Test
		@DisplayName("a bulk request that cannot be sent counts the whole batch as failed")
		void unsendableBulkCountsTheWholeBatchAsFailed() throws Exception {
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(2L);
			stubOnePageOfIds(2);
			when(documentDataService.getBeneficiariesBatch(any()))
					.thenReturn(Arrays.asList(document("1"), document("2")));
			when(esClient.bulk(any(BulkRequest.class))).thenThrow(new IOException("index unavailable"));

			ElasticsearchSyncService.SyncResult result = service.syncAllBeneficiaries();

			assertEquals(0, result.getSuccessCount());
			assertEquals(2, result.getFailureCount());
		}

		@Test
		@DisplayName("the run stops at the first empty page rather than paging to the declared total")
		void runStopsAtTheFirstEmptyPage() throws Exception {
			// The count and the paging query can disagree - rows are deleted
			// while a rebuild runs - so the empty page is the real terminator.
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(1_000_000L);
			when(transactionalWrapper.getBeneficiaryIdsBatch(anyInt(), anyInt()))
					.thenReturn(Collections.emptyList());

			ElasticsearchSyncService.SyncResult result = service.syncAllBeneficiaries();

			assertEquals(0, result.getSuccessCount());
			verify(transactionalWrapper, org.mockito.Mockito.times(1)).getBeneficiaryIdsBatch(anyInt(), anyInt());
		}

		@Test
		@DisplayName("beneficiaries carrying an ABHA identifier are loaded through the enriching batch query")
		void abhaCarryingBeneficiariesAreLoadedEnriched() throws Exception {
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(1L);
			stubOnePageOfIds(1);
			when(documentDataService.getBeneficiariesBatch(any()))
					.thenReturn(Collections.singletonList(documentWithAbha("4001")));
			stubBulkSuccess();

			ElasticsearchSyncService.SyncResult result = service.syncAllBeneficiaries();

			assertEquals(1, result.getSuccessCount());
			verify(documentDataService).getBeneficiariesBatch(any());
		}

		@Test
		@DisplayName("the id types the paging query can return are all accepted")
		void everyIdTypeIsAccepted() throws Exception {
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(4L);
			when(transactionalWrapper.getBeneficiaryIdsBatch(eq(0), anyInt())).thenReturn(
					Arrays.asList(new Object[] { BigInteger.valueOf(1L) }, new Object[] { Long.valueOf(2L) },
							new Object[] { Integer.valueOf(3) }, new Object[] { "not-an-id" },
							new Object[] { null }));
			when(transactionalWrapper.getBeneficiaryIdsBatch(eq(10000), anyInt()))
					.thenReturn(Collections.emptyList());
			when(documentDataService.getBeneficiariesBatch(any()))
					.thenReturn(Collections.singletonList(document("1")));
			stubBulkSuccess();

			service.syncAllBeneficiaries();

			ArgumentCaptor<List<BigInteger>> captor = captor();
			verify(documentDataService).getBeneficiariesBatch(captor.capture());
			assertEquals(Arrays.asList(BigInteger.ONE, BigInteger.TWO, BigInteger.valueOf(3L)), captor.getValue());
		}

		@Test
		@DisplayName("a page query is retried before the run is abandoned")
		void pageQueryIsRetriedBeforeAbandoningTheRun() throws Exception {
			java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(1L);
			when(transactionalWrapper.getBeneficiaryIdsBatch(anyInt(), anyInt())).thenAnswer(invocation -> {
				int offset = invocation.getArgument(0);
				if (offset > 0) {
					return Collections.emptyList();
				}
				if (attempts.getAndIncrement() == 0) {
					throw new IllegalStateException("connection reset");
				}
				return Collections.singletonList(new Object[] { BigInteger.ONE });
			});
			when(documentDataService.getBeneficiariesBatch(any()))
					.thenReturn(Collections.singletonList(document("1")));
			stubBulkSuccess();

			ElasticsearchSyncService.SyncResult result = service.syncAllBeneficiaries();

			assertEquals(1, result.getSuccessCount());
			assertEquals(2, attempts.get(), "the failed page query should have been retried once");
		}

		@Test
		@DisplayName("a page query that keeps failing reports the error in the result rather than throwing")
		void persistentlyFailingPageQueryIsReportedInTheResult() throws Exception {
			// The caller is an HTTP endpoint that reports sync progress; an
			// exception here would surface as a 500 with no partial counts.
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(1L);
			when(transactionalWrapper.getBeneficiaryIdsBatch(anyInt(), anyInt()))
					.thenThrow(new IllegalStateException("connection reset"));

			ElasticsearchSyncService.SyncResult result = service.syncAllBeneficiaries();

			assertNotNull(result.getError());
			assertTrue(result.toString().contains("error"), result.toString());
		}

		@Test
		@DisplayName("a failing count is reported in the result")
		void failingCountIsReportedInTheResult() {
			when(transactionalWrapper.countActiveBeneficiaries())
					.thenThrow(new IllegalStateException("table locked"));

			ElasticsearchSyncService.SyncResult result = service.syncAllBeneficiaries();

			assertEquals("table locked", result.getError());
		}
	}

	@Nested
	@DisplayName("single beneficiary sync")
	class SingleBeneficiarySync {

		@BeforeEach
		void stubIndexCall() throws IOException {
			when(esClient.index(any(Function.class))).thenAnswer(invocation -> {
				@SuppressWarnings({ "unchecked", "rawtypes" })
				Function<IndexRequest.Builder, ObjectBuilder<IndexRequest>> builder = invocation.getArgument(0);
				builder.apply(new IndexRequest.Builder<>()).build();
				return IndexResponse.of(r -> r.index("beneficiary").id("4001").version(1L)
						.result(Result.Created).seqNo(1L).primaryTerm(1L)
						.shards(s -> s.total(1).successful(1).failed(0)));
			});
		}

		@Test
		@DisplayName("an existing beneficiary is fetched and indexed")
		void existingBeneficiaryIsFetchedAndIndexed() {
			when(transactionalWrapper.existsByBenRegId(BigInteger.valueOf(100200300L))).thenReturn(true);
			when(documentDataService.getBeneficiaryFromDatabase(BigInteger.valueOf(100200300L)))
					.thenReturn(documentWithAbha("4001"));

			assertTrue(service.syncSingleBeneficiary("100200300"));
		}

		@Test
		@DisplayName("a beneficiary that is not in the database is not indexed")
		void beneficiaryNotInTheDatabaseIsNotIndexed() {
			when(transactionalWrapper.existsByBenRegId(any())).thenReturn(false);

			assertFalse(service.syncSingleBeneficiary("100200300"));
			verify(documentDataService, never()).getBeneficiaryFromDatabase(any());
		}

		@Test
		@DisplayName("a beneficiary whose document cannot be built is not indexed")
		void beneficiaryWithNoDocumentIsNotIndexed() throws Exception {
			when(transactionalWrapper.existsByBenRegId(any())).thenReturn(true);
			when(documentDataService.getBeneficiaryFromDatabase(any())).thenReturn(null);

			assertFalse(service.syncSingleBeneficiary("100200300"));
			verify(esClient, never()).index(any(Function.class));
		}

		@Test
		@DisplayName("a document with no beneficiary id is not indexed")
		void documentWithNoIdIsNotIndexed() throws Exception {
			when(transactionalWrapper.existsByBenRegId(any())).thenReturn(true);
			when(documentDataService.getBeneficiaryFromDatabase(any())).thenReturn(document(null));

			assertFalse(service.syncSingleBeneficiary("100200300"));
			verify(esClient, never()).index(any(Function.class));
		}

		@Test
		@DisplayName("a non-numeric identifier is rejected")
		void nonNumericIdentifierIsRejected() {
			assertFalse(service.syncSingleBeneficiary("not-a-number"));
		}

		@Test
		@DisplayName("an index failure is reported rather than thrown")
		void indexFailureIsReported() throws Exception {
			when(transactionalWrapper.existsByBenRegId(any())).thenReturn(true);
			when(documentDataService.getBeneficiaryFromDatabase(any())).thenReturn(document("4001"));
			when(esClient.index(any(Function.class))).thenThrow(new IOException("index unavailable"));

			assertFalse(service.syncSingleBeneficiary("100200300"));
		}
	}

	@Nested
	@DisplayName("sync status")
	class SyncStatusReport {

		private void stubCount(long esCount) throws IOException {
			when(esClient.count(any(Function.class))).thenAnswer(invocation -> {
				@SuppressWarnings("unchecked")
				Function<CountRequest.Builder, ObjectBuilder<CountRequest>> builder = invocation.getArgument(0);
				builder.apply(new CountRequest.Builder()).build();
				return CountResponse.of(c -> c.count(esCount).shards(s -> s.total(1).successful(1).failed(0)));
			});
		}

		@Test
		@DisplayName("matching counts report the index as in sync")
		void matchingCountsReportInSync() throws Exception {
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(500L);
			stubCount(500L);

			ElasticsearchSyncService.SyncStatus status = service.checkSyncStatus();

			assertTrue(status.isSynced());
			assertEquals(0L, status.getMissingCount());
			assertEquals(500L, status.getDatabaseCount());
			assertEquals(500L, status.getElasticsearchCount());
			assertTrue(status.toString().contains("500"));
		}

		@Test
		@DisplayName("a shortfall in the index is reported as the number of missing documents")
		void shortfallIsReportedAsMissingDocuments() throws Exception {
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(500L);
			stubCount(480L);

			ElasticsearchSyncService.SyncStatus status = service.checkSyncStatus();

			assertFalse(status.isSynced());
			assertEquals(20L, status.getMissingCount());
		}

		@Test
		@DisplayName("an unreachable index reports the error rather than a false zero")
		void unreachableIndexReportsTheError() throws Exception {
			// A zero elasticsearchCount would read as "everything is missing" and
			// could trigger an unnecessary full rebuild.
			when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(500L);
			when(esClient.count(any(Function.class))).thenThrow(new IOException("index unavailable"));

			ElasticsearchSyncService.SyncStatus status = service.checkSyncStatus();

			assertNotNull(status.getError());
			assertTrue(status.toString().contains("error"), status.toString());
		}
	}

	@Test
	@DisplayName("the result accounting exposes both counts and any error")
	void resultAccountingExposesCountsAndError() {
		ElasticsearchSyncService.SyncResult result = new ElasticsearchSyncService.SyncResult();

		result.addSuccess(5);
		result.addFailure();
		result.addFailure(2);
		result.setError("partial run");

		assertEquals(5, result.getSuccessCount());
		assertEquals(3, result.getFailureCount());
		assertEquals("partial run", result.getError());
		assertTrue(result.toString().contains("5"));
	}

	@Test
	@DisplayName("the status report exposes its counts through its accessors")
	void statusReportExposesItsCounts() {
		ElasticsearchSyncService.SyncStatus status = new ElasticsearchSyncService.SyncStatus();

		status.setDatabaseCount(10L);
		status.setElasticsearchCount(8L);
		status.setMissingCount(2L);
		status.setSynced(false);
		status.setError(null);

		assertEquals(10L, status.getDatabaseCount());
		assertEquals(8L, status.getElasticsearchCount());
		assertEquals(2L, status.getMissingCount());
		assertFalse(status.isSynced());
		assertNotNull(status.toString());
	}

	@SuppressWarnings("unchecked")
	private <T> ArgumentCaptor<List<T>> captor() {
		return ArgumentCaptor.forClass(List.class);
	}
}
