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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;
import com.iemr.common.identity.data.elasticsearch.ElasticsearchSyncJob;
import com.iemr.common.identity.repo.elasticsearch.SyncJobRepo;

/**
 * Tests for the resumable, job-tracked variant of the index rebuild.
 *
 * <p>
 * Unlike the fire-and-forget sync, this one records its progress on a job row
 * so an interrupted rebuild of ~800k beneficiaries can pick up where it left
 * off instead of starting over. The behaviour worth protecting is that
 * bookkeeping: a resume must continue from the stored offset and keep the
 * counts already accumulated, a run of consecutive batch failures must park the
 * job as STALLED rather than spin, and any terminal state must be written back
 * so the status endpoint stops reporting the job as running.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BeneficiaryElasticsearchIndexServiceTest {

	@Mock
	private ElasticsearchClient esClient;
	@Mock
	private BeneficiaryTransactionHelper transactionalWrapper;
	@Mock
	private BeneficiaryDocumentDataService dataService;
	@Mock
	private SyncJobRepo syncJobRepository;

	@InjectMocks
	private BeneficiaryElasticsearchIndexService service;

	private static final Long JOB_ID = 55L;
	/** Matches BATCH_SIZE in the service. */
	private static final int BATCH_SIZE = 2000;

	@BeforeEach
	void configureIndexName() throws IOException {
		ReflectionTestUtils.setField(service, "beneficiaryIndex", "beneficiary");
		when(syncJobRepository.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
		when(esClient.bulk(any(BulkRequest.class)))
				.thenReturn(BulkResponse.of(b -> b.took(1).errors(false).items(Collections.emptyList())));
	}

	private ElasticsearchSyncJob job(String status) {
		ElasticsearchSyncJob job = new ElasticsearchSyncJob();
		job.setJobId(JOB_ID);
		job.setJobType("FULL_SYNC");
		job.setStatus(status);
		job.setProcessedRecords(0L);
		job.setSuccessCount(0L);
		job.setFailureCount(0L);
		job.setCurrentOffset(0);
		return job;
	}

	private BeneficiaryDocument document(String benId) {
		BeneficiaryDocument document = new BeneficiaryDocument();
		document.setBenId(benId);
		return document;
	}

	private List<Object[]> idPage(int count) {
		return IntStream.range(0, count).mapToObj(index -> new Object[] { BigInteger.valueOf(1000L + index) })
				.collect(Collectors.toList());
	}

	/** Returns one page of ids at offset 0, then nothing. */
	private void stubSinglePage(int count) {
		when(transactionalWrapper.getBeneficiaryIdsBatch(anyInt(), anyInt())).thenAnswer(invocation -> {
			int offset = invocation.getArgument(0);
			return offset == 0 ? idPage(count) : Collections.emptyList();
		});
	}

	@Test
	@DisplayName("an unknown job id is rejected before any work starts")
	void unknownJobIdIsRejected() {
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.empty());

		assertThrows(RuntimeException.class, () -> service.syncAllBeneficiariesAsync(JOB_ID, "admin"));
		verify(transactionalWrapper, never()).countActiveBeneficiaries();
	}

	@Test
	@DisplayName("a fresh job is marked running and stamped with a start time")
	void freshJobIsMarkedRunning() {
		ElasticsearchSyncJob job = job("PENDING");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(1L);
		stubSinglePage(1);
		when(dataService.getBeneficiariesBatch(any())).thenReturn(Collections.singletonList(document("1")));

		service.syncAllBeneficiariesAsync(JOB_ID, "admin");

		assertNotNull(job.getStartedAt());
		assertEquals("COMPLETED", job.getStatus());
		assertEquals(1L, job.getSuccessCount());
		assertEquals(0L, job.getFailureCount());
	}

	@Test
	@DisplayName("an empty database completes the job with an explanatory message")
	void emptyDatabaseCompletesTheJobWithAMessage() {
		ElasticsearchSyncJob job = job("PENDING");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(0L);

		service.syncAllBeneficiariesAsync(JOB_ID, "admin");

		assertEquals("COMPLETED", job.getStatus());
		assertEquals("No beneficiaries found to sync", job.getErrorMessage());
		assertNotNull(job.getCompletedAt());
		verify(transactionalWrapper, never()).getBeneficiaryIdsBatch(anyInt(), anyInt());
	}

	@Test
	@DisplayName("a total already recorded on the job is reused instead of re-counting")
	void recordedTotalIsReused() {
		// The count is a full table scan; a resumed job must not pay for it
		// again.
		ElasticsearchSyncJob job = job("PENDING");
		job.setTotalRecords(1L);
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		stubSinglePage(1);
		when(dataService.getBeneficiariesBatch(any())).thenReturn(Collections.singletonList(document("1")));

		service.syncAllBeneficiariesAsync(JOB_ID, "admin");

		verify(transactionalWrapper, never()).countActiveBeneficiaries();
	}

	@Test
	@DisplayName("a running job with a stored offset resumes from it and keeps its counts")
	void runningJobResumesFromStoredOffset() {
		ElasticsearchSyncJob job = job("RUNNING");
		job.setCurrentOffset(BATCH_SIZE);
		job.setTotalRecords((long) BATCH_SIZE * 2);
		job.setProcessedRecords(1500L);
		job.setSuccessCount(1400L);
		job.setFailureCount(100L);
		job.setStartedAt(new Timestamp(System.currentTimeMillis() - 60_000L));
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		when(transactionalWrapper.getBeneficiaryIdsBatch(anyInt(), anyInt())).thenAnswer(invocation -> {
			int offset = invocation.getArgument(0);
			return offset == BATCH_SIZE ? idPage(1) : Collections.emptyList();
		});
		when(dataService.getBeneficiariesBatch(any())).thenReturn(Collections.singletonList(document("1")));

		service.syncAllBeneficiariesAsync(JOB_ID, "AUTO_RESUME");

		assertEquals("COMPLETED", job.getStatus());
		assertEquals(1401L, job.getSuccessCount());
		assertEquals(100L, job.getFailureCount());
		assertEquals(1501L, job.getProcessedRecords());
		verify(transactionalWrapper, never()).getBeneficiaryIdsBatch(org.mockito.ArgumentMatchers.eq(0), anyInt());
	}

	@Test
	@DisplayName("a document with no beneficiary id is counted as a failure")
	void documentWithNoIdIsCountedAsFailure() {
		ElasticsearchSyncJob job = job("PENDING");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(2L);
		stubSinglePage(2);
		when(dataService.getBeneficiariesBatch(any()))
				.thenReturn(Arrays.asList(document("1"), document(null), null));

		service.syncAllBeneficiariesAsync(JOB_ID, "admin");

		assertEquals(1L, job.getSuccessCount());
		assertEquals(2L, job.getFailureCount());
	}

	@Test
	@DisplayName("beneficiaries the batch query did not return are counted as failures")
	void beneficiariesNotReturnedAreCountedAsFailures() {
		ElasticsearchSyncJob job = job("PENDING");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(3L);
		stubSinglePage(3);
		when(dataService.getBeneficiariesBatch(any())).thenReturn(Collections.singletonList(document("1")));

		service.syncAllBeneficiariesAsync(JOB_ID, "admin");

		assertEquals(1L, job.getSuccessCount());
		assertEquals(2L, job.getFailureCount());
	}

	@Test
	@DisplayName("a batch whose ids all fail conversion is skipped without loading documents")
	void batchWithNoConvertibleIdsIsSkipped() {
		ElasticsearchSyncJob job = job("PENDING");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(1L);
		when(transactionalWrapper.getBeneficiaryIdsBatch(anyInt(), anyInt())).thenAnswer(invocation -> {
			int offset = invocation.getArgument(0);
			return offset == 0 ? Collections.singletonList(new Object[] { null }) : Collections.emptyList();
		});

		service.syncAllBeneficiariesAsync(JOB_ID, "admin");

		assertEquals("COMPLETED", job.getStatus());
		verify(dataService, never()).getBeneficiariesBatch(any());
	}

	@ParameterizedTest
	@ValueSource(strings = { "big-integer", "long", "integer", "short", "text", "unparseable" })
	@DisplayName("every id type the native query can return is converted or skipped")
	void everyIdTypeIsConvertedOrSkipped(String kind) {
		Object id;
		switch (kind) {
		case "big-integer":
			id = BigInteger.valueOf(1L);
			break;
		case "long":
			id = Long.valueOf(1L);
			break;
		case "integer":
			id = Integer.valueOf(1);
			break;
		case "short":
			id = Short.valueOf((short) 1);
			break;
		case "text":
			id = "1";
			break;
		default:
			id = "not-a-number";
		}
		ElasticsearchSyncJob job = job("PENDING");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(1L);
		final Object idValue = id;
		when(transactionalWrapper.getBeneficiaryIdsBatch(anyInt(), anyInt())).thenAnswer(invocation -> {
			int offset = invocation.getArgument(0);
			return offset == 0 ? Collections.singletonList(new Object[] { idValue }) : Collections.emptyList();
		});
		when(dataService.getBeneficiariesBatch(any())).thenReturn(Collections.singletonList(document("1")));

		service.syncAllBeneficiariesAsync(JOB_ID, "admin");

		assertEquals("COMPLETED", job.getStatus());
	}

	@Test
	@DisplayName("a single failing batch is skipped and the job still completes")
	void singleFailingBatchIsSkipped() {
		ElasticsearchSyncJob job = job("PENDING");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		when(transactionalWrapper.countActiveBeneficiaries()).thenReturn((long) BATCH_SIZE * 2);
		when(transactionalWrapper.getBeneficiaryIdsBatch(anyInt(), anyInt())).thenAnswer(invocation -> {
			int offset = invocation.getArgument(0);
			if (offset == 0) {
				throw new IllegalStateException("connection reset");
			}
			return offset == BATCH_SIZE ? idPage(1) : Collections.emptyList();
		});
		when(dataService.getBeneficiariesBatch(any())).thenReturn(Collections.singletonList(document("1")));

		service.syncAllBeneficiariesAsync(JOB_ID, "admin");

		assertEquals("COMPLETED", job.getStatus());
		assertEquals(1L, job.getSuccessCount());
	}

	@Test
	@DisplayName("a run of consecutive batch failures parks the job as stalled")
	void consecutiveBatchFailuresParkTheJobAsStalled() {
		// Backoff caps at 10s per error, so a permanently broken query would
		// otherwise hold the executor thread indefinitely.
		ElasticsearchSyncJob job = job("PENDING");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		when(transactionalWrapper.countActiveBeneficiaries()).thenReturn((long) BATCH_SIZE * 20);
		when(transactionalWrapper.getBeneficiaryIdsBatch(anyInt(), anyInt()))
				.thenThrow(new IllegalStateException("connection reset"));

		service.syncAllBeneficiariesAsync(JOB_ID, "admin");

		assertEquals("STALLED", job.getStatus());
		assertTrue(job.getErrorMessage().contains("Too many consecutive errors"), job.getErrorMessage());
	}

	@Test
	@DisplayName("a failure outside the batch loop marks the job failed with its reason")
	void failureOutsideTheBatchLoopMarksTheJobFailed() {
		ElasticsearchSyncJob job = job("PENDING");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		when(transactionalWrapper.countActiveBeneficiaries())
				.thenThrow(new IllegalStateException("table locked"));

		service.syncAllBeneficiariesAsync(JOB_ID, "admin");

		assertEquals("FAILED", job.getStatus());
		assertEquals("table locked", job.getErrorMessage());
		assertNotNull(job.getCompletedAt());
	}

	@Test
	@DisplayName("a completed job records the throughput it achieved")
	void completedJobRecordsItsThroughput() {
		ElasticsearchSyncJob job = job("PENDING");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(1L);
		stubSinglePage(1);
		when(dataService.getBeneficiariesBatch(any())).thenReturn(Collections.singletonList(document("1")));

		service.syncAllBeneficiariesAsync(JOB_ID, "admin");

		assertNotNull(job.getProcessingSpeed());
		assertEquals(1, job.getCurrentOffset());
	}

	@Test
	@DisplayName("a stalled job can be resumed")
	void stalledJobCanBeResumed() {
		ElasticsearchSyncJob job = job("STALLED");
		job.setCurrentOffset(BATCH_SIZE);
		job.setTotalRecords((long) BATCH_SIZE);
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));

		service.resumeStalledJob(JOB_ID);

		assertEquals("COMPLETED", job.getStatus());
	}

	@ParameterizedTest
	@ValueSource(strings = { "COMPLETED", "FAILED", "PENDING", "CANCELLED" })
	@DisplayName("a job that is not stalled or running cannot be resumed")
	void jobThatIsNotStalledOrRunningCannotBeResumed(String status) {
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job(status)));

		RuntimeException thrown = assertThrows(RuntimeException.class, () -> service.resumeStalledJob(JOB_ID));

		assertTrue(thrown.getMessage().contains("Cannot resume job"), thrown.getMessage());
	}

	@Test
	@DisplayName("resuming an unknown job id is rejected")
	void resumingUnknownJobIdIsRejected() {
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.empty());

		assertThrows(RuntimeException.class, () -> service.resumeStalledJob(JOB_ID));
	}

	@Test
	@DisplayName("a bulk request that cannot be sent counts the batch as failed and still completes the job")
	void unsendableBulkStillCompletesTheJob() throws Exception {
		ElasticsearchSyncJob job = job("PENDING");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job));
		when(transactionalWrapper.countActiveBeneficiaries()).thenReturn(1L);
		stubSinglePage(1);
		when(dataService.getBeneficiariesBatch(any())).thenReturn(Collections.singletonList(document("1")));
		when(esClient.bulk(any(BulkRequest.class))).thenThrow(new IOException("index unavailable"));

		service.syncAllBeneficiariesAsync(JOB_ID, "admin");

		assertEquals("COMPLETED", job.getStatus());
		assertEquals(0L, job.getSuccessCount());
		assertEquals(1L, job.getFailureCount());
	}
}
