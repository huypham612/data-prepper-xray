/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.converter;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.source.dynamodb.configuration.StreamConfig;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StreamRecordConverter extends RecordConverter {
    private static final Logger LOG = LoggerFactory.getLogger(StreamRecordConverter.class);


    static final String CHANGE_EVENTS_PROCESSED_COUNT = "changeEventsProcessed";
    static final String CHANGE_EVENTS_PROCESSING_ERROR_COUNT = "changeEventsProcessingErrors";
    static final String BYTES_RECEIVED = "bytesReceived";
    static final String BYTES_PROCESSED = "bytesProcessed";

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    private final StreamConfig streamConfig;

    private final PluginMetrics pluginMetrics;

    private final Counter changeEventSuccessCounter;
    private final Counter changeEventErrorCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;

    private Instant currentSecond;
    private int recordsSeenThisSecond = 0;

    public StreamRecordConverter(final BufferAccumulator<org.opensearch.dataprepper.model.record.Record<Event>> bufferAccumulator,
                                 final TableInfo tableInfo,
                                 final PluginMetrics pluginMetrics,
                                 final StreamConfig streamConfig) {
        super(bufferAccumulator, tableInfo);
        this.pluginMetrics = pluginMetrics;
        this.changeEventSuccessCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSED_COUNT);
        this.changeEventErrorCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSING_ERROR_COUNT);
        this.bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        this.bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
        this.streamConfig = streamConfig;

    }

    @Override
    String getEventType() {
        return "STREAM";
    }


    public void writeToBuffer(final AcknowledgementSet acknowledgementSet, List<Record> records) {

        int eventCount = 0;
        for (Record record : records) {
            final long bytes = record.dynamodb().sizeBytes();
            Map<String, Object> data;
            Map<String, Object> keys;
            try {
                final Map<String, AttributeValue> streamRecord = getStreamRecordFromImage(record);

                // NewImage may be empty
                data = convertData(streamRecord);
                // Always get keys from dynamodb().keys()
                keys = convertKeys(record.dynamodb().keys());
            } catch (final Exception e) {
                LOG.error("Failed to parse and convert data from stream due to {}", e.getMessage());
                changeEventErrorCounter.increment();
                continue;
            }

            try {
                bytesReceivedSummary.record(bytes);
                final long eventCreationTimeMillis = calculateTieBreakingVersionFromTimestamp(record.dynamodb().approximateCreationDateTime());
                addToBuffer(acknowledgementSet, data, keys, record.dynamodb().approximateCreationDateTime().toEpochMilli(), eventCreationTimeMillis, record.eventNameAsString(), record.userIdentity());
                bytesProcessedSummary.record(bytes);
                eventCount++;
            } catch (Exception e) {
                // will this cause too many logs?
                LOG.error("Failed to add event to buffer due to {}", e.getMessage());
                changeEventErrorCounter.increment();
            }
        }

        try {
            flushBuffer();
            changeEventSuccessCounter.increment(eventCount);
        } catch (Exception e) {
            LOG.error("Failed to write {} events to buffer due to {}", eventCount, e.getMessage());
            changeEventErrorCounter.increment(eventCount);
        }
    }

    /**
     * Process the DynamoDB attributes to be formatted correctly
     */
    private Object processAttributeValue(AttributeValue attributeValue){
        switch (attributeValue.type()){
            case N:     // N for number
                return new BigDecimal(attributeValue.n());
            case B:     // B for Binary
                return BASE64_ENCODER.encodeToString(attributeValue.b().asByteArray());
            case S:     // S for String
                return attributeValue.s();
            case BOOL:  // BOOL for Boolean
                return attributeValue.bool();
            case NS:    // NS for Number Set
                return attributeValue.ns().stream()
                        .map(BigDecimal::new).collect(Collectors.toSet());
            case BS:    // BS for Binary Set
                return attributeValue.bs().stream()
                        .map(buffer -> BASE64_ENCODER.encodeToString(buffer.asByteArray()))
                        .collect(Collectors.toSet());
            case SS:    // SS for String Set
                return attributeValue.ss();
            case L:     // L for List
                return convertListData(attributeValue.l());
            case M:     // M for Map
                return convertData(attributeValue.m());
            case NUL:  // NUL for Null
                return null;
            default:
                throw new IllegalArgumentException("Unsupported attribute type: " + attributeValue.type());
        }
    }

    /**
     * Convert the DynamoDB attribute map to a normal map for data
     */
    private Map<String, Object> convertData(Map<String, AttributeValue> data) {
        return data.entrySet().stream()
                .collect(HashMap::new,
                        (map, entry) -> map.put(entry.getKey(), processAttributeValue(entry.getValue())),
                        HashMap::putAll);
    }

    /**
     * Convert the DynamoDB attribute List to a normal list for data
     */
    private List<Object> convertListData(List<AttributeValue> data) {
        return data.stream()
                .map(this::processAttributeValue)
                .collect(Collectors.toList());
    }

    /**
     * Convert the DynamoDB attribute map to a normal map for keys
     * This method may not be necessary, can use convertData() alternatively
     */
    private Map<String, Object> convertKeys(Map<String, AttributeValue> keys) {
        Map<String, Object> result = new HashMap<>();
        // The attribute type for key can only be N, B or S
        keys.forEach(((attributeName, attributeValue) -> {
            if (attributeValue.type() == AttributeValue.Type.N) {
                // N for number
                result.put(attributeName, attributeValue.n());
            } else if (attributeValue.type() == AttributeValue.Type.B) {
                // B for Binary
                result.put(attributeName, attributeValue.b().toString());
            } else {
                result.put(attributeName, attributeValue.s());
            }
        }));
        return result;

    }

    private long calculateTieBreakingVersionFromTimestamp(final Instant eventTimeInSeconds) {
        if (currentSecond == null) {
            currentSecond = eventTimeInSeconds;
        } else if (currentSecond.isAfter(eventTimeInSeconds)) {
            return eventTimeInSeconds.getEpochSecond() * 1_000_000;
        } else if (currentSecond.isBefore(eventTimeInSeconds)) {
            recordsSeenThisSecond = 0;
            currentSecond = eventTimeInSeconds;
        } else {
            recordsSeenThisSecond++;
        }

        return eventTimeInSeconds.getEpochSecond() * 1_000_000 + recordsSeenThisSecond;
    }

    private Map<String, AttributeValue> getStreamRecordFromImage(final Record record) {
        if (!OperationType.REMOVE.equals(record.eventName())) {
            return record.dynamodb().newImage();
        }

        if (StreamViewType.OLD_IMAGE.equals(streamConfig.getStreamViewForRemoves())) {
            if (!record.dynamodb().hasOldImage()) {
                LOG.warn("view_on_remove with OLD_IMAGE is enabled, but no old image can be found on the stream record, using NEW_IMAGE");
                return record.dynamodb().newImage();
            } else {
                return record.dynamodb().oldImage();
            }
        }

        return record.dynamodb().newImage();
    }
}
