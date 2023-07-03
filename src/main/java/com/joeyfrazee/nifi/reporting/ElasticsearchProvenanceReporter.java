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

import co.elastic.clients.transport.TransportUtils;
import java.io.*;
import java.net.URL;
import java.util.*;

import javax.net.ssl.SSLContext;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.ReportingContext;

import org.elasticsearch.client.RestClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

@Tags({"elasticsearch", "provenance"})
@CapabilityDescription("A provenance reporting task that writes to Elasticsearch")
public class ElasticsearchProvenanceReporter extends AbstractProvenanceReporter {
    // -------------------------------------------------------------------------
    // CONSTANTS
    // -------------------------------------------------------------------------

    /** The address for Elasticsearch. */
    public static final PropertyDescriptor ELASTICSEARCH_URL = new PropertyDescriptor
            .Builder().name("Elasticsearch URL")
            .displayName("Elasticsearch URL")
            .description("The address for Elasticsearch."+defaultEnvironmentVariableDescription(PluginEnvironmentVariable.ELASTICSEARCH_URL))
            .required(true)
            .defaultValue(PluginEnvironmentVariable.ELASTICSEARCH_URL.getValue().orElse("http://localhost:9200"))
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    /** The name of the Elasticsearch index. */
    public static final PropertyDescriptor ELASTICSEARCH_INDEX = new PropertyDescriptor
            .Builder().name("Elasticsearch Index")
            .displayName("Elasticsearch Index")
            .description("The name of the Elasticsearch index."+defaultEnvironmentVariableDescription(PluginEnvironmentVariable.ELASTICSEARCH_INDEX))
            .required(true)
            .defaultValue(PluginEnvironmentVariable.ELASTICSEARCH_INDEX.getValue().orElse("nifi"))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /** The HTTP CA certificate SHA-256 fingerprint for Elasticsearch. Required for HTTPS. */
    public static final PropertyDescriptor ELASTICSEARCH_CA_CERT_FINGERPRINT = new PropertyDescriptor
            .Builder().name("Elasticsearch CA Certificate Fingerprint")
            .displayName("Elasticsearch CA Certificate Fingerprint")
            .description("The HTTP CA certificate SHA-256 fingerprint for Elasticsearch. Required for HTTPS."+defaultEnvironmentVariableDescription(PluginEnvironmentVariable.ELASTICSEARCH_CA_CERT_FINGERPRINT))
            .defaultValue(PluginEnvironmentVariable.ELASTICSEARCH_CA_CERT_FINGERPRINT.getValue().orElse(null))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /** The username for Elasticsearch authentication. Required for HTTPS */
    public static final PropertyDescriptor ELASTICSEARCH_USERNAME = new PropertyDescriptor
            .Builder().name("Elasticsearch Username")
            .displayName("Elasticsearch Username")
            .description("The username for Elasticsearch authentication. Required for HTTPS."+defaultEnvironmentVariableDescription(PluginEnvironmentVariable.ELASTICSEARCH_USERNAME))
            .defaultValue(PluginEnvironmentVariable.ELASTICSEARCH_USERNAME.getValue().orElse(null))
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /** The password for Elasticsearch authentication. Required for HTTPS. */
    public static final PropertyDescriptor ELASTICSEARCH_PASSWORD = new PropertyDescriptor
            .Builder().name("Elasticsearch Password")
            .displayName("Elasticsearch Password")
            .description("The password for Elasticsearch authentication. Required for HTTPS."+defaultEnvironmentVariableDescription(PluginEnvironmentVariable.ELASTICSEARCH_PASSWORD)+" NOTE: The field will display 'No value set' when set via environment variable.")
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * The default password for Elasticsearch authentication. This value is defined outside the property descriptor,
     * because the NiFi UI leaks default sensitive values.
     */
    private static final String DEFAULT_ELASTICSEARCH_PASSWORD = PluginEnvironmentVariable.ELASTICSEARCH_PASSWORD.getValue().orElse(null);

    // -------------------------------------------------------------------------
    // PUBLIC METHODS
    // -------------------------------------------------------------------------

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        descriptors = super.getSupportedPropertyDescriptors();
        descriptors.add(ELASTICSEARCH_URL);
        descriptors.add(ELASTICSEARCH_INDEX);
        descriptors.add(ELASTICSEARCH_CA_CERT_FINGERPRINT);
        descriptors.add(ELASTICSEARCH_USERNAME);
        descriptors.add(ELASTICSEARCH_PASSWORD);
        return descriptors;
    }

    @Override
    public void indexEvent(final Map<String, Object> event, final ReportingContext context) throws IOException {
        // Get properties from context.
        final URL elasticsearchUrl = new URL(context.getProperty(ELASTICSEARCH_URL).getValue());
        final String elasticsearchIndex = context.getProperty(ELASTICSEARCH_INDEX).evaluateAttributeExpressions()
                .getValue();
        final String elasticsearchCACertFingerprint = context.getProperty(ELASTICSEARCH_CA_CERT_FINGERPRINT).getValue();
        final String elasticsearchUsername = context.getProperty(ELASTICSEARCH_USERNAME).getValue();
        final String elasticsearchPassword = getElasticsearchPassword(context);

        // Create the Elasticsearch API client.
        final String protocol = elasticsearchUrl.getProtocol();
        final RestClient restClient = (protocol.equals("https")) ? getSecureRestClient(elasticsearchUrl, elasticsearchCACertFingerprint, elasticsearchUsername, elasticsearchPassword) : getRestClient(elasticsearchUrl);
        final ElasticsearchClient client = getElasticsearchClient(restClient);

        // Index the event.
        final String id = Long.toString((Long) event.get("event_id"));
        final IndexRequest<Map<String, Object>> indexRequest = new IndexRequest.Builder<Map<String, Object>>()
                .index(elasticsearchIndex)
                .id(id)
                .document(event)
                .build();
        client.index(indexRequest);
    }

    // -------------------------------------------------------------------------
    // PRIVATE METHODS
    // -------------------------------------------------------------------------

    /**
     * Fetch the Elasticsearch password from its property descriptor. If no password is set, the value set within the
     * `ELASTICSEARCH_PASSWORD` environment variable will be returned instead.
     *
     * @param context The reporting context.
     * @return The Elasticsearch password, or null.
     */
    private String getElasticsearchPassword(final ReportingContext context) {
        String elasticsearchPassword = context.getProperty(ELASTICSEARCH_PASSWORD).getValue();
        if (elasticsearchPassword == null) {
            elasticsearchPassword = DEFAULT_ELASTICSEARCH_PASSWORD;
        }
        return elasticsearchPassword;
    }

    /**
     * Create an HTTPS REST client for connecting to the given Elasticsearch URL.
     *
     * @param elasticsearchUrl The Elasticsearch URL.
     * @param elasticsearchCACertFingerprint The Elasticsearch CA certificate SHA-256 fingerprint.
     * @param elasticsearchUsername The Elasticsearch username.
     * @param elasticsearchPassword The Elasticsearch password.
     * @return The RestClient object.
     */
    private RestClient getSecureRestClient(URL elasticsearchUrl, String elasticsearchCACertFingerprint, String elasticsearchUsername, String elasticsearchPassword) {
        final SSLContext sslContext = TransportUtils.sslContextFromCaFingerprint(elasticsearchCACertFingerprint);
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticsearchUsername, elasticsearchPassword));
        return RestClient
                .builder(new HttpHost(elasticsearchUrl.getHost(), elasticsearchUrl.getPort(), "https"))
                .setHttpClientConfigCallback(hc -> hc
                        .setSSLContext(sslContext)
                        .setDefaultCredentialsProvider(credentialsProvider)
                )
                .build();
    }

    /**
     * Create a REST client for connecting to the given Elasticsearch URL.
     *
     * @param elasticsearchUrl The Elasticsearch URL.
     * @return The RestClient object.
     */
    private RestClient getRestClient(URL elasticsearchUrl) {
        return RestClient.builder(new HttpHost(elasticsearchUrl.getHost(), elasticsearchUrl.getPort())).build();
    }

    /**
     * Create an Elasticsearch client from the given Elasticsearch REST client.
     *
     * @param restClient The Elasticsearch REST client.
     * @return The ElasticsearchClient object.
     */
    private ElasticsearchClient getElasticsearchClient(RestClient restClient) {
        final ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }
}
