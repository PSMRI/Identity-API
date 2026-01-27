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

import java.io.IOException;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

	private static final Logger logger = LoggerFactory.getLogger(ElasticsearchConfig.class);

    @Value("${elasticsearch.host}")
    private String esHost;

    @Value("${elasticsearch.port}")
    private int esPort;

    @Value("${elasticsearch.username}")
    private String esUsername;

    @Value("${elasticsearch.password}")
    private String esPassword;

    @Value("${elasticsearch.index.beneficiary}")
    private String indexName;

    @Bean
    public ElasticsearchClient elasticsearchClient() {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            AuthScope.ANY,
            new UsernamePasswordCredentials(esUsername, esPassword)
        );

        RestClient restClient = RestClient.builder(
            new HttpHost(esHost, esPort, "http")
        ).setHttpClientConfigCallback(httpClientBuilder -> 
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        ).build();

        ElasticsearchTransport transport = new RestClientTransport(
            restClient,
            new JacksonJsonpMapper()
        );

        return new ElasticsearchClient(transport);
    }

      @Bean
    public Boolean createIndexMapping(ElasticsearchClient client) throws IOException {
        
        // Check if index exists
        boolean exists = client.indices().exists(e -> e.index(indexName)).value();
        
        if (!exists) {
            client.indices().create(c -> c
                .index(indexName)
                .mappings(m -> m
                    .properties("beneficiaryRegID", p -> p.keyword(k -> k))
                    .properties("firstName", p -> p.text(t -> t
                        .fields("keyword", f -> f.keyword(k -> k))
                        .analyzer("standard")
                    ))
                    .properties("lastName", p -> p.text(t -> t
                        .fields("keyword", f -> f.keyword(k -> k))
                        .analyzer("standard")
                    ))
                    .properties("phoneNum", p -> p.keyword(k -> k))
                    .properties("fatherName", p -> p.text(t -> t.analyzer("standard")))
                    .properties("spouseName", p -> p.text(t -> t.analyzer("standard")))
                    .properties("aadharNo", p -> p.keyword(k -> k))
                    .properties("govtIdentityNo", p -> p.keyword(k -> k))
                )
                .settings(s -> s
                    .numberOfShards("3")
                    .numberOfReplicas("1")
                    .refreshInterval(t -> t.time("1s"))
                )
            );
            
            logger.info("Created Elasticsearch index with proper mappings");
        }
            return true;

    }
}