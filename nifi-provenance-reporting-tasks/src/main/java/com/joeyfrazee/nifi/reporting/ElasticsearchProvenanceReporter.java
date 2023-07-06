/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.joeyfrazee.nifi.reporting;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tags({"elasticsearch", "provenance"})
@CapabilityDescription("A provenance reporting task that writes to Elasticsearch")
public abstract class ElasticsearchProvenanceReporter extends AbstractProvenanceReporter {
    public static final PropertyDescriptor ELASTICSEARCH_URL = new PropertyDescriptor
            .Builder().name("Elasticsearch URL")
            .displayName("Elasticsearch URL")
            .description("The address for Elasticsearch")
            .required(true)
            .defaultValue("http://localhost:9200")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ELASTICSEARCH_INDEX = new PropertyDescriptor
            .Builder().name("Index")
            .displayName("Index")
            .description("The name of the Elasticsearch index")
            .required(true)
            .defaultValue("nifi")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private final Map<String, ElasticsearchClient> esClients = new HashMap<>();

    private ElasticsearchClient getElasticsearchClient(String elasticsearchUrl) throws MalformedURLException {
        if (esClients.containsKey(elasticsearchUrl))
            return esClients.get(elasticsearchUrl);

        URL url = new URL(elasticsearchUrl);
        RestClient restClient = RestClient.builder(new HttpHost(url.getHost(), url.getPort())).build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());

        ElasticsearchClient client = new ElasticsearchClient(transport);
        esClients.put(elasticsearchUrl, client);
        return client;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        descriptors = super.getSupportedPropertyDescriptors();
        descriptors.add(ELASTICSEARCH_URL);
        descriptors.add(ELASTICSEARCH_INDEX);
        return descriptors;
    }

    public void indexEvent(final Map<String, Object> event, final ReportingContext context) throws IOException {
        final String elasticsearchUrl = context.getProperty(ELASTICSEARCH_URL).getValue();
        final String elasticsearchIndex = context.getProperty(ELASTICSEARCH_INDEX).evaluateAttributeExpressions().getValue();
        final ElasticsearchClient client = getElasticsearchClient(elasticsearchUrl);
        final String id = Long.toString((Long) event.get("event_id"));
        final IndexRequest<Map<String, Object>> indexRequest = new
                IndexRequest.Builder<Map<String, Object>>()
                .index(elasticsearchIndex)
                .id(id)
                .document(event)
                .build();
        try {
            client.index(indexRequest);
        } catch (ElasticsearchException ex) {
            getLogger().error("Error while indexing event {}", id, ex);
        }
    }
}
