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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.test.util.ReflectionTestUtils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.ForcemergeRequest;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.util.ObjectBuilder;

/**
 * Tests for the index lifecycle used around a full re-index.
 *
 * <p>
 * A rebuild runs in two phases: create the index with write-optimised settings
 * (refresh disabled, no replicas, async translog) so ~800k documents land
 * quickly, then switch it to read-optimised settings once the data is in. Get
 * that ordering or those settings wrong and either the rebuild takes hours or
 * the index is left permanently un-refreshed and invisible to search - neither
 * of which shows up until production. The tests build the real requests the
 * client would send and assert on the settings they carry.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ElasticsearchIndexingServiceTest {

	@Mock
	private ElasticsearchClient esClient;
	@Mock
	private ElasticsearchIndicesClient indicesClient;
	@Mock
	private ElasticsearchSyncService syncService;

	@InjectMocks
	private ElasticsearchIndexingService service;

	private CreateIndexRequest createRequest;
	private PutIndicesSettingsRequest putSettingsRequest;
	private ForcemergeRequest forcemergeRequest;

	@BeforeEach
	@SuppressWarnings("unchecked")
	void configureClient() throws IOException {
		ReflectionTestUtils.setField(service, "beneficiaryIndex", "beneficiary");
		when(esClient.indices()).thenReturn(indicesClient);

		when(indicesClient.exists(any(Function.class))).thenAnswer(invocation -> {
			Function<ExistsRequest.Builder, ObjectBuilder<ExistsRequest>> builder = invocation.getArgument(0);
			builder.apply(new ExistsRequest.Builder()).build();
			return new BooleanResponse(false);
		});
		when(indicesClient.create(any(Function.class))).thenAnswer(invocation -> {
			Function<CreateIndexRequest.Builder, ObjectBuilder<CreateIndexRequest>> builder = invocation
					.getArgument(0);
			createRequest = builder.apply(new CreateIndexRequest.Builder()).build();
			return CreateIndexResponse.of(r -> r.acknowledged(true).shardsAcknowledged(true).index("beneficiary"));
		});
		when(indicesClient.refresh(any(Function.class))).thenAnswer(invocation -> {
			Function<RefreshRequest.Builder, ObjectBuilder<RefreshRequest>> builder = invocation.getArgument(0);
			builder.apply(new RefreshRequest.Builder()).build();
			return co.elastic.clients.elasticsearch.indices.RefreshResponse
					.of(r -> r.shards(s -> s.total(1.0).successful(1.0).failed(0.0)));
		});
		when(indicesClient.putSettings(any(Function.class))).thenAnswer(invocation -> {
			Function<PutIndicesSettingsRequest.Builder, ObjectBuilder<PutIndicesSettingsRequest>> builder = invocation
					.getArgument(0);
			putSettingsRequest = builder.apply(new PutIndicesSettingsRequest.Builder()).build();
			return co.elastic.clients.elasticsearch.indices.PutIndicesSettingsResponse
					.of(r -> r.acknowledged(true));
		});
		when(indicesClient.forcemerge(any(Function.class))).thenAnswer(invocation -> {
			Function<ForcemergeRequest.Builder, ObjectBuilder<ForcemergeRequest>> builder = invocation.getArgument(0);
			forcemergeRequest = builder.apply(new ForcemergeRequest.Builder()).build();
			return co.elastic.clients.elasticsearch.indices.ForcemergeResponse
					.of(r -> r.shards(s -> s.total(1.0).successful(1.0).failed(0.0)));
		});
	}

	@Test
	@DisplayName("the index is created write-optimised so a full rebuild can keep up")
	void indexIsCreatedWriteOptimised() throws Exception {
		service.createIndexWithMapping();

		assertNotNull(createRequest);
		assertEquals("beneficiary", createRequest.index());
		assertEquals("-1", createRequest.settings().refreshInterval().time());
		assertEquals("1", createRequest.settings().numberOfShards());
		assertEquals("0", createRequest.settings().numberOfReplicas());
		assertEquals(co.elastic.clients.elasticsearch.indices.TranslogDurability.Async,
				createRequest.settings().translog().durability());
	}

	@Test
	@DisplayName("the searchable name fields are mapped for exact, prefix and fuzzy matching")
	void nameFieldsAreMappedForEveryMatchStyle() throws Exception {
		// The universal search issues term, prefix and fuzzy clauses against
		// these fields; a missing sub-field silently returns no hits for one of
		// them.
		service.createIndexWithMapping();

		Map<String, co.elastic.clients.elasticsearch._types.mapping.Property> properties = createRequest.mappings()
				.properties();
		for (String field : new String[] { "firstName", "middleName", "lastName" }) {
			assertNotNull(properties.get(field), field + " is not mapped");
			assertTrue(properties.get(field).text().fields().containsKey("keyword"),
					field + " has no keyword sub-field for exact matching");
			assertTrue(properties.get(field).text().fields().containsKey("prefix"),
					field + " has no prefix sub-field");
		}
		assertNotNull(properties.get("benRegId"));
		assertNotNull(properties.get("beneficiaryID"));
		assertNotNull(properties.get("permVillageName"));
		assertNotNull(properties.get("aadharNo"));
	}

	@Test
	@DisplayName("an existing index is dropped before it is recreated")
	void existingIndexIsDroppedFirst() throws Exception {
		when(indicesClient.exists(any(Function.class))).thenReturn(new BooleanResponse(true));

		service.createIndexWithMapping();

		verify(indicesClient).delete(any(Function.class));
		assertNotNull(createRequest);
	}

	@Test
	@DisplayName("an index that does not exist yet is not deleted")
	void absentIndexIsNotDeleted() throws Exception {
		service.createIndexWithMapping();

		verify(indicesClient, never()).delete(any(Function.class));
	}

	@Test
	@DisplayName("a failure while creating the index is surfaced to the caller")
	void createFailureIsSurfaced() throws Exception {
		when(indicesClient.create(any(Function.class))).thenThrow(new IOException("cluster unavailable"));

		assertThrows(IOException.class, () -> service.createIndexWithMapping());
	}

	@Test
	@DisplayName("optimising for search refreshes, re-settles the settings and merges segments, in that order")
	void optimiseRefreshesThenResettlesThenMerges() throws Exception {
		service.optimizeForSearch();

		org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(indicesClient);
		inOrder.verify(indicesClient).refresh(any(Function.class));
		inOrder.verify(indicesClient).putSettings(any(Function.class));
		inOrder.verify(indicesClient).forcemerge(any(Function.class));
	}

	@Test
	@DisplayName("optimising for search turns refresh and replication back on")
	void optimiseTurnsRefreshAndReplicationBackOn() throws Exception {
		// Leaving refresh at -1 after a rebuild is the failure mode this guards:
		// the documents are in the index but never become searchable.
		service.optimizeForSearch();

		assertEquals("1s", putSettingsRequest.settings().refreshInterval().time());
		assertEquals("1", putSettingsRequest.settings().numberOfReplicas());
		assertEquals(co.elastic.clients.elasticsearch.indices.TranslogDurability.Request,
				putSettingsRequest.settings().translog().durability());
		assertTrue(putSettingsRequest.settings().queries().cache().enabled());
	}

	@Test
	@DisplayName("segments are merged down to one per shard and flushed")
	void segmentsAreMergedToOnePerShard() throws Exception {
		service.optimizeForSearch();

		assertEquals(1L, forcemergeRequest.maxNumSegments());
		assertTrue(forcemergeRequest.flush());
	}

	@Test
	@DisplayName("a failure while optimising is surfaced to the caller")
	void optimiseFailureIsSurfaced() throws Exception {
		when(indicesClient.refresh(any(Function.class))).thenThrow(new IOException("cluster unavailable"));

		assertThrows(IOException.class, () -> service.optimizeForSearch());
	}

	@Test
	@DisplayName("the full workflow reports the sync counts it achieved")
	void fullWorkflowReportsSyncCounts() {
		ElasticsearchSyncService.SyncResult result = new ElasticsearchSyncService.SyncResult();
		result.addSuccess(700);
		result.addFailure(3);
		when(syncService.syncAllBeneficiaries()).thenReturn(result);

		Map<String, Integer> response = service.indexAllBeneficiaries();

		assertEquals(700, response.get("success"));
		assertEquals(3, response.get("failed"));
	}

	@Test
	@DisplayName("the full workflow optimises for search once the data is in")
	void fullWorkflowOptimisesAfterSyncing() throws Exception {
		when(syncService.syncAllBeneficiaries()).thenReturn(new ElasticsearchSyncService.SyncResult());

		service.indexAllBeneficiaries();

		org.mockito.InOrder inOrder = org.mockito.Mockito.inOrder(syncService, indicesClient);
		inOrder.verify(syncService).syncAllBeneficiaries();
		inOrder.verify(indicesClient).refresh(any(Function.class));
	}

	@Test
	@DisplayName("a workflow that fails reports zero counts rather than throwing")
	void failingWorkflowReportsZeroCounts() {
		when(syncService.syncAllBeneficiaries()).thenThrow(new IllegalStateException("database unavailable"));

		Map<String, Integer> response = service.indexAllBeneficiaries();

		assertEquals(0, response.get("success"));
		assertEquals(0, response.get("failed"));
	}

	@Test
	@DisplayName("a workflow whose optimisation fails still reports zero rather than partial counts")
	void workflowWithFailingOptimisationReportsZero() throws Exception {
		ElasticsearchSyncService.SyncResult result = new ElasticsearchSyncService.SyncResult();
		result.addSuccess(700);
		when(syncService.syncAllBeneficiaries()).thenReturn(result);
		when(indicesClient.refresh(any(Function.class))).thenThrow(new IOException("cluster unavailable"));

		Map<String, Integer> response = service.indexAllBeneficiaries();

		assertEquals(0, response.get("success"));
	}

	@Test
	@DisplayName("index statistics failures are surfaced rather than reported as empty stats")
	void indexStatisticsFailuresAreSurfaced() throws Exception {
		when(indicesClient.stats(any(Function.class))).thenThrow(new IOException("cluster unavailable"));

		assertThrows(IOException.class, () -> service.getIndexStats());
	}
}
