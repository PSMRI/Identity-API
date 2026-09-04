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
package com.iemr.common.identity.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Executor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Tests for the thread pools the background index rebuilds run on.
 *
 * <p>
 * A rebuild saturates both the database and Elasticsearch, so the sync pool is
 * deliberately narrow and, when its queue fills, rejects new work with an
 * explanatory error rather than silently discarding a job an operator believes
 * is queued.
 */
class ElasticsearchSyncConfigTest {

	private final ElasticsearchSyncConfig config = new ElasticsearchSyncConfig();

	@Test
	@DisplayName("the sync pool is kept narrow so a rebuild cannot saturate the database")
	void syncPoolIsKeptNarrow() {
		ThreadPoolTaskExecutor executor = (ThreadPoolTaskExecutor) config.elasticsearchSyncExecutor();

		assertEquals(2, executor.getCorePoolSize());
		assertEquals(4, executor.getMaxPoolSize());
		assertEquals(60, executor.getKeepAliveSeconds());
		assertTrue(executor.getThreadNamePrefix().startsWith("es-sync-"));
		executor.shutdown();
	}

	@Test
	@DisplayName("work rejected by a full sync queue is reported rather than dropped")
	void rejectedWorkIsReportedRatherThanDropped() {
		ThreadPoolTaskExecutor executor = (ThreadPoolTaskExecutor) config.elasticsearchSyncExecutor();

		RuntimeException thrown = assertThrows(RuntimeException.class, () -> executor
				.getThreadPoolExecutor().getRejectedExecutionHandler().rejectedExecution(() -> {
				}, executor.getThreadPoolExecutor()));

		assertTrue(thrown.getMessage().contains("queue is full"), thrown.getMessage());
		executor.shutdown();
	}

	@Test
	@DisplayName("the general async pool is wider than the sync pool")
	void generalAsyncPoolIsWiderThanTheSyncPool() {
		ThreadPoolTaskExecutor executor = (ThreadPoolTaskExecutor) config.taskExecutor();

		assertEquals(5, executor.getCorePoolSize());
		assertEquals(10, executor.getMaxPoolSize());
		assertTrue(executor.getThreadNamePrefix().startsWith("async-"));
		executor.shutdown();
	}

	@Test
	@DisplayName("both pools are initialised and ready to accept work")
	void bothPoolsAreInitialisedAndReady() throws Exception {
		Executor syncExecutor = config.elasticsearchSyncExecutor();
		Executor asyncExecutor = config.taskExecutor();

		assertNotNull(((ThreadPoolTaskExecutor) syncExecutor).getThreadPoolExecutor());
		assertNotNull(((ThreadPoolTaskExecutor) asyncExecutor).getThreadPoolExecutor());
		((ThreadPoolTaskExecutor) syncExecutor).shutdown();
		((ThreadPoolTaskExecutor) asyncExecutor).shutdown();
	}
}
