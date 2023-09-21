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

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

@Stateful(scopes = Scope.CLUSTER, description = "After querying the "
        + "provenance repository, the last seen event id is stored so "
        + "reporting can persist across restarts of the reporting task or "
        + "NiFi. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation.")
public abstract class AbstractProvenanceReporter extends AbstractReportingTask {

    static final AllowableValue BEGINNING_OF_STREAM = new AllowableValue("beginning-of-stream",
            "Beginning of Stream",
            "Start reading provenance Events from the beginning of the stream (the oldest event first)");

    static final AllowableValue END_OF_STREAM = new AllowableValue("end-of-stream", "End of Stream",
            "Start reading provenance Events from the end of the stream, ignoring old events");

    static final List<String> DEFAULT_DETAILS_AS_ERROR = Arrays.asList(
            "Auto-Terminated by Failure Relationship", "Auto-Terminated by No Retry Relationship"
    );

    static final PropertyDescriptor START_POSITION = new PropertyDescriptor.Builder().name("start-position")
            .displayName("Start Position")
            .description("If the Reporting Task has never been run, or if its state has been reset by a user, "
                    + "specifies where in the stream of Provenance Events the Reporting Task should start")
            .allowableValues(BEGINNING_OF_STREAM, END_OF_STREAM)
            .defaultValue(BEGINNING_OF_STREAM.getValue()).required(true).build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder().name("Batch Size")
            .displayName("Batch Size")
            .description("Specifies how many records to send in a single batch, at most.").required(true)
            .defaultValue("1000").addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).build();

    static final PropertyDescriptor DETAILS_AS_ERROR = new PropertyDescriptor.Builder().name("details-as-error")
            .displayName("Details as error")
            .description("Specifies a comma-separated list of details messages in the provenance event "
                    + "that will be considered as errors (comparison is case-insensitive)")
            .defaultValue(String.join(",", DEFAULT_DETAILS_AS_ERROR))
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.NON_BLANK_VALIDATOR)).build();

    static final PropertyDescriptor NIFI_URL = new PropertyDescriptor.Builder().name("nifi-url")
            .displayName("NiFi URL")
            .description("Specifies the URL of the current NiFi instance. It is later used to create links pointing "
                    + "to specific processors and process groups")
            .defaultValue("https://localhost:443")
            .addValidator(StandardValidators.URL_VALIDATOR).build();

    protected List<PropertyDescriptor> descriptors;

    private volatile ProvenanceEventConsumer consumer;

    final SimpleDateFormat sdf = new SimpleDateFormat ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(START_POSITION);
        descriptors.add(BATCH_SIZE);
        descriptors.add(DETAILS_AS_ERROR);
        descriptors.add(NIFI_URL);
        return descriptors;
    }

    public void createConsumer(final ReportingContext context) {
        if (consumer != null)
            return;
        consumer = new ProvenanceEventConsumer();
        consumer.setStartPositionValue(context.getProperty(START_POSITION).getValue());
        consumer.setBatchSize(context.getProperty(BATCH_SIZE).asInteger());
        consumer.setLogger(getLogger());
        consumer.setScheduled(true);
    }

    private void processProvenanceEvents(ReportingContext context) {
        createConsumer(context);

        final List<String> detailsAsError =
                Arrays.asList(context.getProperty(DETAILS_AS_ERROR).getValue().toLowerCase().split(","));
        final String nifiUrl = context.getProperty(NIFI_URL).getValue();

        consumer.consumeEvents(context, ((componentMapHolder, provenanceEventRecords) -> {
            getLogger().debug("Starting to consume events");
            for (final ProvenanceEventRecord e: provenanceEventRecords) {
                getLogger().debug("Processing provenance event: {}", e.getEventId());
                final Map<String, Object> source = new HashMap<>();

                source.put("@timestamp", sdf.format(new Date()));
                source.put("event_id", e.getEventId());
                source.put("event_time", new Date(e.getEventTime()));
                source.put("entry_date", new Date(e.getFlowFileEntryDate()));
                source.put("lineage_start_date", new Date(e.getLineageStartDate()));
                source.put("file_size", e.getFileSize());

                e.getContentClaimContainer();

                final String componentName = componentMapHolder.getComponentName(e.getComponentId());
                final String processGroupId = componentMapHolder.getProcessGroupId(e.getComponentId(),
                        e.getComponentType());
                final String processGroupName = componentMapHolder.getComponentName(processGroupId);
                source.put("component_name", componentName);
                source.put("process_group_id", processGroupId);
                source.put("process_group_name", processGroupName);

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
                    source.put(
                        "component_url",
                        nifiUrl + "?processGroupId=" + processGroupId + "&componentsIds=" + componentId
                    );
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

                if (details != null && detailsAsError.contains(details.toLowerCase()))
                    source.put("status", "Error");
                else
                    source.put("status", "Info");

                final String relationship = e.getRelationship();
                if (relationship != null) {
                    source.put("relationship", relationship);
                }

                final String sourceQueueId = e.getSourceQueueIdentifier();
                if (sourceQueueId != null) {
                    source.put("source_queue_id", sourceQueueId);
                }

                final Map<String, String> updatedAttributes = e.getUpdatedAttributes();
                if (updatedAttributes != null && !updatedAttributes.isEmpty()) {
                    source.put("updatedAttributes", updatedAttributes);
                }

                final Map<String, String> previousAttributes = e.getPreviousAttributes();
                if (previousAttributes != null && !previousAttributes.isEmpty()) {
                    source.put("previousAttributes", previousAttributes);
                }

                // TO get URL Prefix, we just remove the /nifi from the end of the URL
                final String urlPrefix = nifiUrl.substring(0, nifiUrl.length() - "/nifi".length());
                final String downloadContentUri = urlPrefix + "/nifi-api/provenance-events/" + e.getEventId() + "/content";
                source.put("download_input_content_uri", downloadContentUri + "/input");
                source.put("download_output_content_uri", downloadContentUri + "/output");
                final String viewContentUri =
                    urlPrefix + "/nifi-content-viewer/" +
                    "?ref=" + urlPrefix + "/nifi-api/provenance-events/" + e.getEventId() + "/content";
                source.put("view_input_content_uri", viewContentUri + "/input");
                source.put("view_output_content_uri", viewContentUri + "/output");

                try {
                    indexEvent(source, context);
                } catch (IOException ex) {
                    getLogger().error("Failed to publish provenance event", e);
                }
            }
        }));
    }

    public abstract void indexEvent(final Map<String, Object> event, final ReportingContext context) throws IOException;

    @Override
    public void onTrigger(final ReportingContext context) {
        getLogger().debug("Triggering provenance events reporting");
        final boolean isClustered = context.isClustered();
        final String nodeId = context.getClusterNodeIdentifier();
        if (nodeId == null && isClustered) {
            getLogger().debug(
                    "This instance of NiFi is configured for clustering, but the Cluster Node Identifier is not yet available. "
                            + "Will wait for Node Identifier to be established.");
            return;
        }

        try {
            processProvenanceEvents(context);
        } catch (final Exception e) {
            getLogger().error("Failed to process provenance events", e);
        }
    }
}
