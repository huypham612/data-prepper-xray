/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.apache.commons.codec.DecoderException;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.trace.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputCodec for OTLP Traces that encodes Span events into OTLP Protobuf binary format
 */
@DataPrepperPlugin(name = "otlp_trace", pluginType = OutputCodec.class)
public class OTelProtoTraceOutputCodec implements OutputCodec {

    private static final String OTLP_EXTENSION = "otlp";
    private static final Logger LOG = LoggerFactory.getLogger(OTelProtoTraceOutputCodec.class);;

    private final OTelProtoStandardCodec.OTelProtoEncoder encoder = new OTelProtoStandardCodec.OTelProtoEncoder();

    @Override
    public void start(OutputStream outputStream, Event event, OutputCodecContext context) {
        // no-op for OTLP
    }

    @Override
    public void writeEvent(Event event, OutputStream outputStream) throws IOException {
        if (!(event instanceof Span)) {
            throw new IllegalArgumentException("OtlpTraceOutputCodec only supports Span events");
        }

        Span span = (Span) event;
        try {
            ResourceSpans resourceSpans = encoder.convertToResourceSpans(span);
            ExportTraceServiceRequest request = ExportTraceServiceRequest.newBuilder()
                    .addResourceSpans(resourceSpans)
                    .build();

            outputStream.write(request.toByteArray());
        } catch (DecoderException e) {
            LOG.warn("Skipping invalid span with ID [{}] due to decoding error.", span.getSpanId(), e);
        }
    }


    @Override
    public void complete(OutputStream outputStream) {
        // no-op for OTLP
    }

    @Override
    public String getExtension() {
        return OTLP_EXTENSION;
    }
}

