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

import java.util.*;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.searchbox.core.*;
import io.searchbox.client.*;
import io.searchbox.client.config.*;

@Tags({"elasticsearch", "provenance"})
@CapabilityDescription("A provenance reporting task that writes to Elasticsearch")
public class ElasticsearchProvenanceReporter extends AbstractReportingTask {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchProvenanceReporter.class);

    private AtomicLong lastEventId = new AtomicLong(0);

    public static final PropertyDescriptor ELASTICSEARCH_URL = new PropertyDescriptor
            .Builder().name("Elasticsearch URL")
            .description("The address for Elasticsearch")
            .required(true)
            .defaultValue("http://localhost:9200")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor ELASTICSEARCH_INDEX = new PropertyDescriptor
            .Builder().name("Index")
            .description("The name of the Elasticsearch index")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ELASTICSEARCH_DOC_TYPE = new PropertyDescriptor
            .Builder().name("Document Type")
            .description("The type of documents to insert into the index")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor
            .Builder().name("Page Size")
            .description("Page size for scrolling through the provenance repository")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_HISTORY = new PropertyDescriptor
            .Builder().name("Maximum History")
            .description(
                "How far back to look into the provenance repository to " +
                "index provenance events"
            )
            .required(true)
            .defaultValue("10000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    private List<PropertyDescriptor> descriptors;

    private long getLastEventId() {
        return lastEventId.get();
    }

    private void setLastEventId(long eventId) {
        lastEventId.set(eventId);
    }

    private JestClient getJestClient(String elasticsearch) {
        JestClientFactory factory = new JestClientFactory();

        factory.setHttpClientConfig(
            new HttpClientConfig.Builder(elasticsearch)
                .multiThreaded(true)
                .build()
        );

        JestClient client = factory.getObject();

        return client;
    }

    private Map<String, Object> createSourceDocument(ProvenanceEventRecord e) {
        final Map<String, Object> source = new HashMap<String, Object>();

        source.put("event_id", Long.valueOf(e.getEventId()));
        source.put("event_time", new Date(e.getEventTime()));
        source.put("entry_date", new Date(e.getFlowFileEntryDate()));
        source.put("lineage_start_date", new Date(e.getLineageStartDate()));
        source.put("file_size", Long.valueOf(e.getFileSize()));

        final Long previousFileSize = e.getPreviousFileSize();
        if (previousFileSize != null && previousFileSize >= 0) {
            source.put("previous_file_size", previousFileSize);
        }

        final long eventDuration = e.getEventDuration();
        if (eventDuration >= 0) {
            source.put("event_duration_millis", eventDuration);
            source.put("event_duration_seconds", eventDuration / 1000);
        }

        final ProvenanceEventType eventType = e.getEventType();
        if (eventType != null) {
            source.put("event_type", eventType.toString());
        }

        final String componentId = e.getComponentId();
        if (componentId != null) {
            source.put("component_id", componentId);
        }

        final String componentType = e.getComponentType();
        if (componentType != null) {
            source.put("component_type", componentType);
        }

        final String sourceSystemId = e.getSourceSystemFlowFileIdentifier();
        if (sourceSystemId != null) {
            source.put("source_system_id", sourceSystemId);
        }

        final String flowFileId = e.getFlowFileUuid();
        if (flowFileId != null) {
            source.put("flow_file_id", flowFileId);
        }

        final List<String> parentIds = e.getParentUuids();
        if (parentIds != null && !parentIds.isEmpty()) {
            source.put("parent_ids", parentIds);
        }

        final List<String> childIds = e.getChildUuids();
        if (childIds != null && !childIds.isEmpty()) {
            source.put("child_ids", childIds);
        }

        final String details = e.getDetails();
        if (details != null) {
            source.put("details", details);
        }

        final String relationship = e.getRelationship();
        if (relationship != null) {
            source.put("relationship", relationship);
        }

        final String sourceQueueId = e.getSourceQueueIdentifier();
        if (sourceQueueId != null) {
            source.put("source_queue_id", sourceQueueId);
        }

        final Map<String, String> attributes = new HashMap<String, String>();

        final Map<String, String> receivedAttributes = e.getAttributes();
        if (receivedAttributes != null && !receivedAttributes.isEmpty()) {
            attributes.putAll(receivedAttributes);
        }

        final Map<String, String> updatedAttributes = e.getUpdatedAttributes();
        if (updatedAttributes != null && !updatedAttributes.isEmpty()) {
            attributes.putAll(updatedAttributes);
        }

        source.put("attributes", attributes);

        return source;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(ELASTICSEARCH_URL);
        descriptors.add(ELASTICSEARCH_INDEX);
        descriptors.add(ELASTICSEARCH_DOC_TYPE);
        descriptors.add(PAGE_SIZE);
        descriptors.add(MAX_HISTORY);
        return descriptors;
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final EventAccess access = context.getEventAccess();
        final ProvenanceEventRepository provenance = access.getProvenanceRepository();
        final Long maxEventId = provenance.getMaxEventId();

        final String elasticsearchUrl = context.getProperty(ELASTICSEARCH_URL).getValue();

        final String elasticsearchIndex = context.getProperty(ELASTICSEARCH_INDEX).getValue();

        final String elasticsearchType = context.getProperty(ELASTICSEARCH_DOC_TYPE).getValue();

        final int pageSize = Integer.parseInt(context.getProperty(PAGE_SIZE).getValue());

        final int maxHistory = Integer.parseInt(context.getProperty(MAX_HISTORY).getValue());

        final JestClient client = getJestClient(elasticsearchUrl);

        try {
            while (maxEventId != null && getLastEventId() < maxEventId.longValue()) {
                if ((maxEventId.longValue() - getLastEventId()) > maxHistory) {
                    setLastEventId(maxEventId.longValue() - maxHistory);
                }

                final List<ProvenanceEventRecord> events = provenance.getEvents(getLastEventId(), pageSize);

                for (ProvenanceEventRecord e : events) {
                    final Map<String, Object> source = createSourceDocument(e);

                    final Index index = new Index.Builder(source)
                        .index(elasticsearchIndex)
                        .type(elasticsearchType)
                        .id(Long.toString(e.getEventId()))
                        .build();

                    client.execute(index);
                }

                setLastEventId(Math.min(getLastEventId() + pageSize, maxEventId.longValue()));
            }
        }
        catch (IOException e) {
            logger.error(e.getMessage(), e);
            return;
        }
    }
}
