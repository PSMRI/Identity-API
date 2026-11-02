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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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

import com.iemr.common.identity.data.elasticsearch.ElasticsearchSyncJob;
import com.iemr.common.identity.repo.elasticsearch.SyncJobRepo;

/**
 * Tests for the sync-job bookkeeping behind the admin sync endpoints.
 *
 * <p>
 * A full re-index takes tens of minutes and saturates the database, so the
 * important guarantees are that only one can be running at a time, that a job
 * can only be resumed or cancelled from a state where that makes sense, and
 * that the async worker is only handed a job that has actually been persisted.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SyncJobServiceTest {

	@Mock
	private SyncJobRepo syncJobRepository;
	@Mock
	private BeneficiaryElasticsearchIndexService syncService;

	@InjectMocks
	private SyncJobService service;

	private static final Long JOB_ID = 55L;

	@BeforeEach
	void stubSave() {
		when(syncJobRepository.save(any())).thenAnswer(invocation -> {
			ElasticsearchSyncJob job = invocation.getArgument(0);
			if (job.getJobId() == null) {
				job.setJobId(JOB_ID);
			}
			return job;
		});
	}

	private ElasticsearchSyncJob job(String status) {
		ElasticsearchSyncJob job = new ElasticsearchSyncJob();
		job.setJobId(JOB_ID);
		job.setJobType("FULL_SYNC");
		job.setStatus(status);
		job.setCurrentOffset(2000);
		return job;
	}

	@Test
	@DisplayName("a new full sync is persisted with zeroed counters before the worker starts")
	void newFullSyncIsPersistedBeforeTheWorkerStarts() {
		// The worker looks the job up by id, so it has to exist first.
		when(syncJobRepository.hasActiveFullSyncJob()).thenReturn(false);

		ElasticsearchSyncJob job = service.startFullSyncJob("admin");

		assertEquals("FULL_SYNC", job.getJobType());
		assertEquals("PENDING", job.getStatus());
		assertEquals("admin", job.getTriggeredBy());
		assertEquals(0L, job.getProcessedRecords());
		assertEquals(0L, job.getSuccessCount());
		assertEquals(0L, job.getFailureCount());
		assertEquals(0, job.getCurrentOffset());
		org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(syncJobRepository, syncService);
		inOrder.verify(syncJobRepository).save(any());
		inOrder.verify(syncService).syncAllBeneficiariesAsync(JOB_ID, "admin");
	}

	@Test
	@DisplayName("a second full sync is refused while one is already running")
	void secondFullSyncIsRefusedWhileOneIsRunning() {
		when(syncJobRepository.hasActiveFullSyncJob()).thenReturn(true);

		RuntimeException thrown = assertThrows(RuntimeException.class, () -> service.startFullSyncJob("admin"));

		assertTrue(thrown.getMessage().contains("already running"), thrown.getMessage());
		verify(syncService, never()).syncAllBeneficiariesAsync(anyLong(), anyString());
		verify(syncJobRepository, never()).save(any());
	}

	@Test
	@DisplayName("a failed job is resumed from its stored offset")
	void failedJobIsResumedFromItsStoredOffset() {
		ElasticsearchSyncJob failed = job("FAILED");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(failed));

		ElasticsearchSyncJob resumed = service.resumeJob(JOB_ID, "admin");

		assertEquals("PENDING", resumed.getStatus());
		assertEquals("admin", resumed.getTriggeredBy());
		assertEquals(2000, resumed.getCurrentOffset());
		verify(syncService).syncAllBeneficiariesAsync(JOB_ID, "admin");
	}

	@ParameterizedTest
	@ValueSource(strings = { "RUNNING", "PENDING", "COMPLETED", "CANCELLED", "STALLED" })
	@DisplayName("only a failed job can be resumed")
	void onlyFailedJobsCanBeResumed(String status) {
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(job(status)));

		RuntimeException thrown = assertThrows(RuntimeException.class, () -> service.resumeJob(JOB_ID, "admin"));

		assertTrue(thrown.getMessage().contains("Can only resume FAILED jobs"), thrown.getMessage());
		verify(syncService, never()).syncAllBeneficiariesAsync(anyLong(), anyString());
	}

	@Test
	@DisplayName("resuming an unknown job id is rejected")
	void resumingUnknownJobIdIsRejected() {
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.empty());

		assertThrows(RuntimeException.class, () -> service.resumeJob(JOB_ID, "admin"));
	}

	@ParameterizedTest
	@ValueSource(strings = { "RUNNING", "PENDING" })
	@DisplayName("an active job is cancelled and stamped with a completion time")
	void activeJobIsCancelled(String status) {
		ElasticsearchSyncJob active = job(status);
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(active));

		assertTrue(service.cancelJob(JOB_ID));

		assertEquals("CANCELLED", active.getStatus());
		assertNotNull(active.getCompletedAt());
	}

	@ParameterizedTest
	@ValueSource(strings = { "COMPLETED", "FAILED", "CANCELLED" })
	@DisplayName("a job that is no longer active cannot be cancelled")
	void inactiveJobCannotBeCancelled(String status) {
		ElasticsearchSyncJob finished = job(status);
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(finished));

		assertFalse(service.cancelJob(JOB_ID));

		assertEquals(status, finished.getStatus());
		verify(syncJobRepository, never()).save(any());
	}

	@Test
	@DisplayName("cancelling an unknown job id reports failure rather than throwing")
	void cancellingUnknownJobIdReportsFailure() {
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.empty());

		assertFalse(service.cancelJob(JOB_ID));
	}

	@Test
	@DisplayName("a job's status is looked up by its id")
	void jobStatusIsLookedUpById() {
		ElasticsearchSyncJob running = job("RUNNING");
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.of(running));

		assertEquals(running, service.getJobStatus(JOB_ID));
	}

	@Test
	@DisplayName("asking for an unknown job's status is rejected")
	void unknownJobStatusIsRejected() {
		when(syncJobRepository.findByJobId(JOB_ID)).thenReturn(Optional.empty());

		assertThrows(RuntimeException.class, () -> service.getJobStatus(JOB_ID));
	}

	@Test
	@DisplayName("active and recent job listings are passed straight through")
	void jobListingsArePassedStraightThrough() {
		List<ElasticsearchSyncJob> active = Collections.singletonList(job("RUNNING"));
		List<ElasticsearchSyncJob> recent = Arrays.asList(job("COMPLETED"), job("FAILED"));
		when(syncJobRepository.findActiveJobs()).thenReturn(active);
		when(syncJobRepository.findRecentJobs()).thenReturn(recent);

		assertEquals(active, service.getActiveJobs());
		assertEquals(recent, service.getRecentJobs());
	}

	@Test
	@DisplayName("whether a full sync is running is answered from the repository")
	void fullSyncRunningIsAnsweredFromTheRepository() {
		when(syncJobRepository.hasActiveFullSyncJob()).thenReturn(true);

		assertTrue(service.isFullSyncRunning());
	}

	@Test
	@DisplayName("the latest job of a type is the first the repository returns")
	void latestJobOfATypeIsTheFirstReturned() {
		// The query orders newest first, so position matters.
		ElasticsearchSyncJob newest = job("COMPLETED");
		when(syncJobRepository.findLatestJobsByType("FULL_SYNC"))
				.thenReturn(Arrays.asList(newest, job("FAILED")));

		assertEquals(newest, service.getLatestJobByType("FULL_SYNC"));
	}

	@Test
	@DisplayName("a type with no jobs yet yields nothing rather than an error")
	void typeWithNoJobsYieldsNothing() {
		when(syncJobRepository.findLatestJobsByType("INCREMENTAL_SYNC")).thenReturn(Collections.emptyList());

		assertNull(service.getLatestJobByType("INCREMENTAL_SYNC"));
	}
}
