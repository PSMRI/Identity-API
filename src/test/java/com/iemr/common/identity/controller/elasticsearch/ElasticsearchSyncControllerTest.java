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
package com.iemr.common.identity.controller.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.elasticsearch.indices.RefreshResponse;
import co.elastic.clients.util.ObjectBuilder;

import com.iemr.common.identity.data.elasticsearch.ElasticsearchSyncJob;
import com.iemr.common.identity.domain.MBeneficiarydetail;
import com.iemr.common.identity.domain.MBeneficiarymapping;
import com.iemr.common.identity.repo.BenMappingRepo;
import com.iemr.common.identity.service.elasticsearch.ElasticsearchIndexingService;
import com.iemr.common.identity.service.elasticsearch.ElasticsearchSyncService;
import com.iemr.common.identity.service.elasticsearch.SyncJobService;
import com.iemr.common.identity.utils.response.OutputResponse;

/**
 * Tests for the operations endpoints that drive an index rebuild.
 *
 * <p>
 * These are the endpoints an operator hits during an incident, so the status
 * code carries the meaning: a rebuild refused because one is already running is
 * a 409 rather than a 500, an unknown job is a 404, and a rebuild that finished
 * with partial failures is a 206 so a dashboard does not report it as clean.
 * The tests pin those mappings along with the payload each one returns.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ElasticsearchSyncControllerTest {

	@Mock
	private ElasticsearchSyncService syncService;
	@Mock
	private SyncJobService syncJobService;
	@Mock
	private BenMappingRepo mappingRepo;
	@Mock
	private ElasticsearchIndexingService indexingService;
	@Mock
	private ElasticsearchClient esClient;
	@Mock
	private ElasticsearchIndicesClient indicesClient;

	@InjectMocks
	private ElasticsearchSyncController controller;

	private static final Long JOB_ID = 55L;

	@BeforeEach
	void configureController() {
		ReflectionTestUtils.setField(controller, "beneficiaryIndex", "beneficiary");
		when(esClient.indices()).thenReturn(indicesClient);
	}

	private ElasticsearchSyncJob job(String status) {
		ElasticsearchSyncJob job = new ElasticsearchSyncJob();
		job.setJobId(JOB_ID);
		job.setJobType("FULL_SYNC");
		job.setStatus(status);
		job.setTotalRecords(1000L);
		job.setProcessedRecords(400L);
		job.setSuccessCount(390L);
		job.setFailureCount(10L);
		job.setCurrentOffset(400);
		job.setProcessingSpeed(120.5);
		job.setEstimatedTimeRemaining(5L);
		job.setStartedAt(new Timestamp(System.currentTimeMillis()));
		return job;
	}

	@Nested
	@DisplayName("starting a rebuild")
	class StartingARebuild {

		@Test
		@DisplayName("a started rebuild returns its job id and where to poll it")
		void startedRebuildReturnsJobIdAndPollUrl() {
			when(syncJobService.startFullSyncJob("API")).thenReturn(job("PENDING"));

			ResponseEntity<Map<String, Object>> response = controller.startAsyncFullSync("API");

			assertEquals(HttpStatus.OK, response.getStatusCode());
			assertEquals("success", response.getBody().get("status"));
			assertEquals(JOB_ID, response.getBody().get("jobId"));
			assertEquals("PENDING", response.getBody().get("jobStatus"));
			assertEquals("/elasticsearch/status/55", response.getBody().get("checkStatusUrl"));
		}

		@Test
		@DisplayName("a rebuild refused because one is already running answers 409, not 500")
		void refusedRebuildAnswersConflict() {
			// A dashboard retries on 5xx; 409 tells it to wait instead.
			when(syncJobService.startFullSyncJob(anyString()))
					.thenThrow(new RuntimeException("A full sync job is already running."));

			ResponseEntity<Map<String, Object>> response = controller.startAsyncFullSync("API");

			assertEquals(HttpStatus.CONFLICT, response.getStatusCode());
			assertEquals("error", response.getBody().get("status"));
			assertTrue(response.getBody().get("message").toString().contains("already running"));
		}

		@Test
		@DisplayName("the triggering user is recorded with the job")
		void triggeringUserIsRecorded() {
			when(syncJobService.startFullSyncJob("ops.admin")).thenReturn(job("PENDING"));

			controller.startAsyncFullSync("ops.admin");

			verify(syncJobService).startFullSyncJob("ops.admin");
		}
	}

	@Nested
	@DisplayName("job status and listings")
	class JobStatusAndListings {

		@Test
		@DisplayName("a job's progress is reported with a formatted percentage")
		void jobProgressIsReportedWithFormattedPercentage() {
			when(syncJobService.getJobStatus(JOB_ID)).thenReturn(job("RUNNING"));

			ResponseEntity<Map<String, Object>> response = controller.getAsyncJobStatus(JOB_ID);

			assertEquals(HttpStatus.OK, response.getStatusCode());
			assertEquals("RUNNING", response.getBody().get("status"));
			assertEquals(1000L, response.getBody().get("totalRecords"));
			assertEquals(390L, response.getBody().get("successCount"));
			assertEquals("40.00", response.getBody().get("progressPercentage"));
			assertEquals(120.5, response.getBody().get("processingSpeed"));
			assertEquals(5L, response.getBody().get("estimatedTimeRemaining"));
		}

		@Test
		@DisplayName("an unknown job id answers 404")
		void unknownJobIdAnswersNotFound() {
			when(syncJobService.getJobStatus(JOB_ID)).thenThrow(new RuntimeException("Job not found: 55"));

			ResponseEntity<Map<String, Object>> response = controller.getAsyncJobStatus(JOB_ID);

			assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
			assertEquals("error", response.getBody().get("status"));
		}

		@Test
		@DisplayName("active and recent job listings are returned as-is")
		void jobListingsAreReturnedAsIs() {
			List<ElasticsearchSyncJob> active = Collections.singletonList(job("RUNNING"));
			List<ElasticsearchSyncJob> recent = Collections.singletonList(job("COMPLETED"));
			when(syncJobService.getActiveJobs()).thenReturn(active);
			when(syncJobService.getRecentJobs()).thenReturn(recent);

			assertEquals(active, controller.getActiveJobs().getBody());
			assertEquals(recent, controller.getRecentJobs().getBody());
		}
	}

	@Nested
	@DisplayName("resuming and cancelling")
	class ResumingAndCancelling {

		@Test
		@DisplayName("a resumed job reports the offset it will continue from")
		void resumedJobReportsItsOffset() {
			when(syncJobService.resumeJob(JOB_ID, "API")).thenReturn(job("PENDING"));

			ResponseEntity<Map<String, Object>> response = controller.resumeJob(JOB_ID, "API");

			assertEquals(HttpStatus.OK, response.getStatusCode());
			assertEquals(400, response.getBody().get("resumedFromOffset"));
		}

		@Test
		@DisplayName("a job that cannot be resumed answers 400 with the reason")
		void unresumableJobAnswersBadRequest() {
			when(syncJobService.resumeJob(anyLong(), anyString()))
					.thenThrow(new RuntimeException("Can only resume FAILED jobs. Current status: RUNNING"));

			ResponseEntity<Map<String, Object>> response = controller.resumeJob(JOB_ID, "API");

			assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
			assertTrue(response.getBody().get("message").toString().contains("Can only resume FAILED"));
		}

		@Test
		@DisplayName("a cancelled job answers 200")
		void cancelledJobAnswersOk() {
			when(syncJobService.cancelJob(JOB_ID)).thenReturn(true);

			ResponseEntity<Map<String, Object>> response = controller.cancelJob(JOB_ID);

			assertEquals(HttpStatus.OK, response.getStatusCode());
			assertEquals("success", response.getBody().get("status"));
		}

		@Test
		@DisplayName("a job that cannot be cancelled answers 400")
		void uncancellableJobAnswersBadRequest() {
			when(syncJobService.cancelJob(JOB_ID)).thenReturn(false);

			ResponseEntity<Map<String, Object>> response = controller.cancelJob(JOB_ID);

			assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
			assertTrue(response.getBody().get("message").toString().contains("may not be active"));
		}
	}

	@Nested
	@DisplayName("the blocking legacy rebuild")
	class BlockingLegacyRebuild {

		@Test
		@DisplayName("a clean run answers 200 with its counts and a warning about blocking")
		void cleanRunAnswersOk() {
			ElasticsearchSyncService.SyncResult result = new ElasticsearchSyncService.SyncResult();
			result.addSuccess(700);
			when(syncService.syncAllBeneficiaries()).thenReturn(result);

			ResponseEntity<Map<String, Object>> response = controller.syncAllBeneficiaries();

			assertEquals(HttpStatus.OK, response.getStatusCode());
			assertEquals("completed", response.getBody().get("status"));
			assertEquals(700, response.getBody().get("successCount"));
			assertNotNull(response.getBody().get("warning"));
		}

		@Test
		@DisplayName("a run that hit an error answers 206 so it is not reported as clean")
		void runWithAnErrorAnswersPartialContent() {
			ElasticsearchSyncService.SyncResult result = new ElasticsearchSyncService.SyncResult();
			result.addSuccess(400);
			result.setError("connection reset");
			when(syncService.syncAllBeneficiaries()).thenReturn(result);

			ResponseEntity<Map<String, Object>> response = controller.syncAllBeneficiaries();

			assertEquals(HttpStatus.PARTIAL_CONTENT, response.getStatusCode());
			assertEquals("connection reset", response.getBody().get("error"));
		}

		@Test
		@DisplayName("an unexpected failure answers 500")
		void unexpectedFailureAnswersServerError() {
			when(syncService.syncAllBeneficiaries()).thenThrow(new IllegalStateException("out of memory"));

			ResponseEntity<Map<String, Object>> response = controller.syncAllBeneficiaries();

			assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
		}
	}

	@Nested
	@DisplayName("single beneficiary sync")
	class SingleBeneficiarySync {

		@Test
		@DisplayName("a synced beneficiary answers 200 and says so")
		void syncedBeneficiaryAnswersOk() {
			when(syncService.syncSingleBeneficiary("100200300")).thenReturn(true);

			ResponseEntity<Map<String, Object>> response = controller.syncSingleBeneficiary("100200300");

			assertEquals(HttpStatus.OK, response.getStatusCode());
			assertEquals("success", response.getBody().get("status"));
			assertEquals(true, response.getBody().get("synced"));
		}

		@Test
		@DisplayName("a beneficiary that could not be synced still answers 200, reporting the failure in the body")
		void unsyncedBeneficiaryAnswersOkWithFailureInBody() {
			when(syncService.syncSingleBeneficiary("100200300")).thenReturn(false);

			ResponseEntity<Map<String, Object>> response = controller.syncSingleBeneficiary("100200300");

			assertEquals(HttpStatus.OK, response.getStatusCode());
			assertEquals("failed", response.getBody().get("status"));
			assertEquals(false, response.getBody().get("synced"));
			assertTrue(response.getBody().get("message").toString().contains("not found"));
		}

		@Test
		@DisplayName("an unexpected failure answers 500")
		void unexpectedFailureAnswersServerError() {
			when(syncService.syncSingleBeneficiary(anyString()))
					.thenThrow(new IllegalStateException("index unavailable"));

			ResponseEntity<Map<String, Object>> response = controller.syncSingleBeneficiary("100200300");

			assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
			assertEquals(false, response.getBody().get("synced"));
		}
	}

	@Nested
	@DisplayName("status, health and diagnostics")
	class StatusHealthAndDiagnostics {

		@Test
		@DisplayName("the sync status comparison is returned as-is")
		void syncStatusComparisonIsReturnedAsIs() {
			ElasticsearchSyncService.SyncStatus status = new ElasticsearchSyncService.SyncStatus();
			status.setDatabaseCount(500L);
			status.setElasticsearchCount(500L);
			status.setSynced(true);
			when(syncService.checkSyncStatus()).thenReturn(status);

			ResponseEntity<ElasticsearchSyncService.SyncStatus> response = controller.checkSyncStatus();

			assertEquals(HttpStatus.OK, response.getStatusCode());
			assertTrue(response.getBody().isSynced());
		}

		@Test
		@DisplayName("a failing status check answers 500 carrying the error")
		void failingStatusCheckAnswersServerError() {
			when(syncService.checkSyncStatus()).thenThrow(new IllegalStateException("index unavailable"));

			ResponseEntity<ElasticsearchSyncService.SyncStatus> response = controller.checkSyncStatus();

			assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
			assertNotNull(response.getBody().getError());
		}

		@Test
		@DisplayName("the health check reports whether a rebuild is in flight")
		void healthCheckReportsWhetherARebuildIsInFlight() {
			when(syncJobService.isFullSyncRunning()).thenReturn(true);
			when(syncJobService.getActiveJobs()).thenReturn(Collections.singletonList(job("RUNNING")));

			ResponseEntity<Map<String, Object>> response = controller.healthCheck();

			assertEquals("UP", response.getBody().get("status"));
			assertEquals(true, response.getBody().get("asyncJobsRunning"));
			assertEquals(1, response.getBody().get("activeJobs"));
		}

		@Test
		@DisplayName("the diagnostic check reports what the database holds for a beneficiary")
		void diagnosticCheckReportsWhatTheDatabaseHolds() {
			MBeneficiarymapping mapping = new MBeneficiarymapping();
			mapping.setBenMapId(BigInteger.ONE);
			mapping.setDeleted(false);
			mapping.setMBeneficiarydetail(new MBeneficiarydetail());
			when(mappingRepo.countActiveByBenRegId(BigInteger.valueOf(100200300L))).thenReturn(1L);
			when(mappingRepo.findByBenRegId(BigInteger.valueOf(100200300L))).thenReturn(mapping);

			ResponseEntity<Map<String, Object>> response = controller.checkBeneficiaryExists("100200300");

			assertEquals(HttpStatus.OK, response.getStatusCode());
			assertEquals(true, response.getBody().get("existsInDatabase"));
			assertEquals(true, response.getBody().get("hasDetails"));
			assertEquals(false, response.getBody().get("hasContact"));
		}

		@Test
		@DisplayName("the diagnostic check says so when a beneficiary is absent")
		void diagnosticCheckSaysSoWhenBeneficiaryIsAbsent() {
			when(mappingRepo.countActiveByBenRegId(any())).thenReturn(0L);

			ResponseEntity<Map<String, Object>> response = controller.checkBeneficiaryExists("100200300");

			assertEquals(false, response.getBody().get("existsInDatabase"));
			assertTrue(response.getBody().get("message").toString().contains("NOT found"));
			verify(mappingRepo, never()).findByBenRegId(any());
		}

		@Test
		@DisplayName("a non-numeric identifier answers 500 rather than being treated as absent")
		void nonNumericIdentifierAnswersServerError() {
			ResponseEntity<Map<String, Object>> response = controller.checkBeneficiaryExists("not-a-number");

			assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
		}
	}

	@Nested
	@DisplayName("index lifecycle endpoints")
	class IndexLifecycleEndpoints {

		@Test
		@DisplayName("creating the index answers 200")
		void creatingTheIndexAnswersOk() throws Exception {
			ResponseEntity<OutputResponse> response = controller.createIndex();

			assertEquals(HttpStatus.OK, response.getStatusCode());
			verify(indexingService).createIndexWithMapping();
			assertTrue(response.getBody().toString().contains("Index created successfully"));
		}

		@Test
		@DisplayName("a failing index creation answers 500 with the reason")
		void failingIndexCreationAnswersServerError() throws Exception {
			org.mockito.Mockito.doThrow(new IOException("cluster unavailable")).when(indexingService)
					.createIndexWithMapping();

			ResponseEntity<OutputResponse> response = controller.createIndex();

			assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
			assertTrue(response.getBody().toString().contains("Error creating index"));
		}

		@Test
		@DisplayName("recreate-and-sync rebuilds the index before loading it, and reports both counts")
		void recreateAndSyncRebuildsThenLoads() throws Exception {
			when(indexingService.indexAllBeneficiaries()).thenReturn(Map.of("success", 700, "failed", 3));

			ResponseEntity<OutputResponse> response = controller.recreateAndSync();

			assertEquals(HttpStatus.OK, response.getStatusCode());
			org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(indexingService);
			inOrder.verify(indexingService).createIndexWithMapping();
			inOrder.verify(indexingService).indexAllBeneficiaries();
			assertTrue(response.getBody().toString().contains("700"));
		}

		@Test
		@DisplayName("a failing recreate-and-sync answers 500 without loading data")
		void failingRecreateAnswersServerErrorWithoutLoading() throws Exception {
			org.mockito.Mockito.doThrow(new IOException("cluster unavailable")).when(indexingService)
					.createIndexWithMapping();

			ResponseEntity<OutputResponse> response = controller.recreateAndSync();

			assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
			verify(indexingService, never()).indexAllBeneficiaries();
		}

		@Test
		@DisplayName("the index-info endpoint answers 200 with its placeholder")
		void indexInfoAnswersOk() {
			ResponseEntity<OutputResponse> response = controller.getIndexInfo();

			assertEquals(HttpStatus.OK, response.getStatusCode());
			assertNotNull(response.getBody());
		}

		@Test
		@DisplayName("a manual refresh makes newly indexed documents searchable")
		@SuppressWarnings("unchecked")
		void manualRefreshMakesDocumentsSearchable() throws Exception {
			when(indicesClient.refresh(any(Function.class))).thenAnswer(invocation -> {
				Function<RefreshRequest.Builder, ObjectBuilder<RefreshRequest>> builder = invocation.getArgument(0);
				RefreshRequest request = builder.apply(new RefreshRequest.Builder()).build();
				assertEquals(Collections.singletonList("beneficiary"), request.index());
				return RefreshResponse.of(r -> r.shards(s -> s.total(1.0).successful(1.0).failed(0.0)));
			});

			ResponseEntity<Map<String, Object>> response = controller.refreshIndex();

			assertEquals(HttpStatus.OK, response.getStatusCode());
			assertEquals("success", response.getBody().get("status"));
		}

		@Test
		@DisplayName("a failing refresh reports the error")
		@SuppressWarnings("unchecked")
		void failingRefreshReportsTheError() throws Exception {
			when(indicesClient.refresh(any(Function.class))).thenThrow(new IOException("cluster unavailable"));

			ResponseEntity<Map<String, Object>> response = controller.refreshIndex();

			assertEquals("error", response.getBody().get("status"));
			assertTrue(response.getBody().get("message").toString().contains("Refresh failed"));
		}
	}
}
