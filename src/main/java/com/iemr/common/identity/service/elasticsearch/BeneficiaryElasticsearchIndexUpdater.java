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

import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.IndexRequest;

import com.iemr.common.identity.data.elasticsearch.BeneficiaryDocument;

/**
 * Service for real-time Elasticsearch synchronization
 * Triggers automatically when beneficiaries are created/updated in database
 */
@Service
public class BeneficiaryElasticsearchIndexUpdater {

    private static final Logger logger = LoggerFactory.getLogger(BeneficiaryElasticsearchIndexUpdater.class);

    @Autowired
    private ElasticsearchClient esClient;

    @Autowired
    private BeneficiaryDocumentDataService dataService;

    @Value("${elasticsearch.index.beneficiary}")
    private String beneficiaryIndex;

    @Value("${elasticsearch.enabled}")
    private boolean esEnabled;

    /**
     * Delete beneficiary from Elasticsearch
     */
    @Async("elasticsearchSyncExecutor")
    public void deleteBeneficiaryAsync(String benId) {
        if (!esEnabled) {
            logger.debug("Elasticsearch is disabled, skipping delete");
            return;
        }

        try {
            logger.info("Starting async delete for benId: {}", benId);

            DeleteRequest request = DeleteRequest.of(d -> d
                    .index(beneficiaryIndex)
                    .id(benId));

            esClient.delete(request);

            logger.info("Successfully deleted beneficiary from Elasticsearch: benId={}", benId);

        } catch (Exception e) {
            logger.error("Error deleting beneficiary {} from Elasticsearch: {}", benId, e.getMessage(), e);
        }
    }

    @Async
    public CompletableFuture<Void> syncBeneficiaryAsync(BigInteger benRegId) {
        BeneficiaryDocument document = dataService.getBeneficiaryWithAbhaDetails(benRegId);
        if (document == null) {
            logger.warn("No data found for benRegId: {}", benRegId);
            return CompletableFuture.completedFuture(null);
        }

        // Log ABHA for verification
        logger.info("Syncing benRegId={} with ABHA: healthID={}, abhaID={}",
                benRegId, document.getHealthID(), document.getAbhaID());

        try{
        // Index to ES
        esClient.index(i -> i
                .index(beneficiaryIndex)
                .id(String.valueOf(benRegId))
                .document(document).refresh(Refresh.True));

        logger.info("Successfully synced benRegId: {} to ES", benRegId);
         } catch (Exception e) {
            logger.error("Error syncing beneficiary {} to Elasticsearch: {}", benRegId, e.getMessage(), e);
        }
        return CompletableFuture.completedFuture(null);

    }
}