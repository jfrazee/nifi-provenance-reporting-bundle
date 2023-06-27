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

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;

@Stateful(scopes = Scope.CLUSTER, description = "After querying the "
        + "provenance repository, the last seen event id is stored so "
        + "reporting can persist across restarts of the reporting task or "
        + "NiFi. To clear the maximum values, clear the state of the processor "
        + "per the State Management documentation.")
public abstract class AbstractProvenanceReporter extends AbstractReportingTask {
    /** The page size for scrolling through the provenance repository. */
    public static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor
            .Builder().name("Page Size")
            .displayName("Page Size")
            .description("Page size for scrolling through the provenance repository.")
            .required(true)
            .defaultValue(PluginEnvironmentVariable.PAGE_SIZE.getValue().orElse("100"))
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    /** How far back to look into the provenance repository to index provenance events. */
    public static final PropertyDescriptor MAX_HISTORY = new PropertyDescriptor
            .Builder().name("Maximum History")
            .displayName("Maximum History")
            .description("How far back to look into the provenance repository to index provenance events.")
            .required(true)
            .defaultValue(PluginEnvironmentVariable.MAXIMUM_HISTORY.getValue().orElse("10000"))
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    /** The list of PropertyDescriptor objects this reporting task supports. */
    protected List<PropertyDescriptor> descriptors;

    /**
     * Get the last event ID from the given state manager.
     *
     * @param stateManager The state manager to get the last event ID of.
     * @return The last event ID of the state manager, or 0 if it cannot be retrieved.
     */
    private long getLastEventId(StateManager stateManager) {
        try {
            final StateMap stateMap = stateManager.getState(Scope.CLUSTER);
            final String lastEventIdStr = stateMap.get("lastEventId");
            final long lastEventId = lastEventIdStr != null ? Long.parseLong(lastEventIdStr) : 0;
            return lastEventId;
        } catch (final IOException ioe) {
            getLogger().warn("Failed to retrieve the last event id from the "
                    + "state manager.", ioe);
            return 0;
        }
    }

    /**
     * Set the last event ID for the given state manager.
     *
     * @param stateManager The state manager to set the last event ID of.
     * @param eventId The event ID to set.
     * @throws IOException If unable to get the state of the state manager.
     */
    private void setLastEventId(StateManager stateManager, long eventId) throws IOException {
        final StateMap stateMap = stateManager.getState(Scope.CLUSTER);
        final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());
        statePropertyMap.put("lastEventId", Long.toString(eventId));
        stateManager.setState(statePropertyMap, Scope.CLUSTER);
    }

    /**
     * Set the given key to the given value for the given map.
     * Keys containing nested values (e.g. "head.tail") are expanded.
     *
     * @param map The map.
     * @param key The key.
     * @param value The value.
     * @param overwrite Whether to overwrite existing nested maps with the given value.
     * @return The modified map.
     */
    private Map<String, Object> setField(Map<String, Object> map, final String key, final Object value, final boolean overwrite) {
        // Match patterns of form "head.tail" to ensure nested values are properly parsed.
        final Pattern pattern = Pattern.compile("^(\\w+)\\.(.*)$");
        final Matcher matcher = pattern.matcher(key);
        if (matcher.find()) {
            // The key points to a nested value, so expand nested values.
            final String head = matcher.group(1);
            final String tail = matcher.group(2);
            Map<String, Object> obj = (Map<String, Object>) map.get(head);
            if (obj == null) {
                obj = new HashMap<>();
            }
            final Object v = setField(obj, tail, value, overwrite);
            map.put(head, v);
        }
        else {
            // The key is a single value, so parse normally.
            final Object obj = map.get(key);
            // Check whether the key points to a nested map.
            if (obj instanceof Map) {
                getLogger().warn("value at " + key + " is a Map");
                // Overwrite the existing map if allowed.
                if (overwrite) {
                    map.put(key, value);
                }
            }
            else {
                // Default to setting key=value.
                map.put(key, value);
            }
        }
        return map;
    }

    /**
     * Set the given key to the given value for the given map.
     * Keys containing nested values (e.g. "head.tail") are expanded.
     *
     * @param map The map.
     * @param key The key.
     * @param value The value.
     * @return The modified map.
     */
    private Map<String, Object> setField(Map<String, Object> map, final String key, final Object value) {
        return setField(map, key, value, false);
    }

    /**
     * Create a map representation of the given provenance event.
     *
     * @param event The event to create a map of.
     * @return A map representation of the event.
     */
    private Map<String, Object> createEventMap(ProvenanceEventRecord event) {
        final Map<String, Object> source = new HashMap<>();
        final SimpleDateFormat ft = new SimpleDateFormat ("YYYY-MM-dd'T'HH:mm:ss.SSS'Z'");

        source.put("@timestamp", ft.format(new Date()));
        source.put("event_id", event.getEventId());
        source.put("event_time", new Date(event.getEventTime()));
        source.put("entry_date", new Date(event.getFlowFileEntryDate()));
        source.put("lineage_start_date", new Date(event.getLineageStartDate()));
        source.put("file_size", event.getFileSize());

        final Long previousFileSize = event.getPreviousFileSize();
        if (previousFileSize != null && previousFileSize >= 0) {
            source.put("previous_file_size", previousFileSize);
        }

        final long eventDuration = event.getEventDuration();
        if (eventDuration >= 0) {
            source.put("event_duration_millis", eventDuration);
            source.put("event_duration_seconds", eventDuration / 1000);
        }

        final ProvenanceEventType eventType = event.getEventType();
        if (eventType != null) {
            source.put("event_type", eventType.toString());
        }

        final String componentId = event.getComponentId();
        if (componentId != null) {
            source.put("component_id", componentId);
        }

        final String componentType = event.getComponentType();
        if (componentType != null) {
            source.put("component_type", componentType);
        }

        final String sourceSystemId = event.getSourceSystemFlowFileIdentifier();
        if (sourceSystemId != null) {
            source.put("source_system_id", sourceSystemId);
        }

        final String flowFileId = event.getFlowFileUuid();
        if (flowFileId != null) {
            source.put("flow_file_id", flowFileId);
        }

        final List<String> parentIds = event.getParentUuids();
        if (parentIds != null && !parentIds.isEmpty()) {
            source.put("parent_ids", parentIds);
        }

        final List<String> childIds = event.getChildUuids();
        if (childIds != null && !childIds.isEmpty()) {
            source.put("child_ids", childIds);
        }

        final String details = event.getDetails();
        if (details != null) {
            source.put("details", details);
        }

        final String relationship = event.getRelationship();
        if (relationship != null) {
            source.put("relationship", relationship);
        }

        final String sourceQueueId = event.getSourceQueueIdentifier();
        if (sourceQueueId != null) {
            source.put("source_queue_id", sourceQueueId);
        }

        final Map<String, Object> attributes = new HashMap<>();

        final Map<String, String> receivedAttributes = event.getAttributes();
        if (receivedAttributes != null && !receivedAttributes.isEmpty()) {
            for (Map.Entry<String, String> attribute : receivedAttributes.entrySet()) {
                setField(attributes, attribute.getKey(), attribute.getValue());
            }
        }

        final Map<String, String> updatedAttributes = event.getUpdatedAttributes();
        if (updatedAttributes != null && !updatedAttributes.isEmpty()) {
            for (Map.Entry<String, String> attribute : updatedAttributes.entrySet()) {
                setField(attributes, attribute.getKey(), attribute.getValue());
            }
        }

        source.put("attributes", attributes);

        return source;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PAGE_SIZE);
        descriptors.add(MAX_HISTORY);
        return descriptors;
    }

    /**
     * Index an event.
     *
     * @param event The event to index.
     * @param context The reporting context.
     * @throws IOException If indexing fails.
     */
    public abstract void indexEvent(final Map<String, Object> event, final ReportingContext context) throws IOException;

    @Override
    public void onTrigger(final ReportingContext context) {
        final StateManager stateManager = context.getStateManager();
        final EventAccess access = context.getEventAccess();
        final ProvenanceEventRepository provenance = access.getProvenanceRepository();
        final Long maxEventId = provenance.getMaxEventId();

        final int pageSize = Integer.parseInt(context.getProperty(PAGE_SIZE).getValue());
        final int maxHistory = Integer.parseInt(context.getProperty(MAX_HISTORY).getValue());

        try {
            long lastEventId = getLastEventId(stateManager);

            getLogger().info("starting event id: " + lastEventId);

            while (maxEventId != null && lastEventId < maxEventId) {
                if (maxHistory > 0 && (maxEventId - lastEventId) > maxHistory) {
                    lastEventId = maxEventId - maxHistory + 1;
                }

                final List<ProvenanceEventRecord> events = provenance.getEvents(lastEventId, pageSize);

                for (ProvenanceEventRecord e : events) {
                    final Map<String, Object> event = createEventMap(e);
                    indexEvent(event, context);
                }

                lastEventId = Math.min(lastEventId + pageSize, maxEventId);
            }

            getLogger().info("ending event id: " + lastEventId);

            setLastEventId(stateManager, lastEventId);
        }
        catch (IOException e) {
            getLogger().error(e.getMessage(), e);
        }
    }
}
