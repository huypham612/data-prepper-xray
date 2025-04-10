package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OTelProtoTraceOutputCodecTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_SPAN_EVENT_JSON_FILE = "test-span-event.json";

    private OTelProtoTraceOutputCodec codec;

    @BeforeEach
    void setup() {
        codec = new OTelProtoTraceOutputCodec();
    }

    private Span buildSpanFromTestFile(String fileName) {
        try (InputStream inputStream = Objects.requireNonNull(
                getClass().getClassLoader().getResourceAsStream(fileName))) {

            Map<String, Object> spanMap = OBJECT_MAPPER.readValue(inputStream, new TypeReference<>() {});
            JacksonSpan.Builder builder = JacksonSpan.builder()
                    .withTraceId((String) spanMap.get("traceId"))
                    .withSpanId((String) spanMap.get("spanId"))
                    .withParentSpanId((String) spanMap.get("parentSpanId"))
                    .withTraceState((String) spanMap.get("traceState"))
                    .withName((String) spanMap.get("name"))
                    .withKind((String) spanMap.get("kind"))
                    .withDurationInNanos(((Number) spanMap.get("durationInNanos")).longValue())
                    .withStartTime((String) spanMap.get("startTime"))
                    .withEndTime((String) spanMap.get("endTime"))
                    .withTraceGroup((String) spanMap.get("traceGroup"));

            Map<String, Object> traceGroupFieldsMap = (Map<String, Object>) spanMap.get("traceGroupFields");
            if (traceGroupFieldsMap != null) {
                builder.withTraceGroupFields(DefaultTraceGroupFields.builder()
                        .withStatusCode((Integer) traceGroupFieldsMap.getOrDefault("statusCode", 0))
                        .withEndTime((String) spanMap.get("endTime"))
                        .withDurationInNanos(((Number) spanMap.get("durationInNanos")).longValue())
                        .build());
            }

            return builder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load span from file", e);
        }
    }

    @Test
    void testWriteEvent_withValidSpanFromTestFile_writesSuccessfully() throws Exception {
        Span span = buildSpanFromTestFile(TEST_SPAN_EVENT_JSON_FILE);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        codec.start(outputStream, span, new OutputCodecContext());
        codec.writeEvent(span, outputStream);
        codec.complete(outputStream);

        byte[] bytes = outputStream.toByteArray();
        assertThat(bytes).isNotEmpty();

        ExportTraceServiceRequest request = ExportTraceServiceRequest.parseFrom(bytes);
        assertThat(request.getResourceSpansCount()).isGreaterThan(0);
    }

    @Test
    void testWriteEvent_withBadTraceId_logsAndSkips() throws Exception {
        Span span = JacksonSpan.builder()
                .withTraceId("bad-trace-id") // not valid hex
                .withSpanId("1234567812345678")
                .withParentSpanId("")
                .withName("InvalidSpan")
                .withKind("INTERNAL")
                .withTraceGroup("InvalidSpan")
                .withStartTime("2020-05-24T14:00:00Z")
                .withEndTime("2020-05-24T14:00:01Z")
                .withDurationInNanos(1_000_000_000L)
                .withTraceGroupFields(DefaultTraceGroupFields.builder()
                        .withStatusCode(0)
                        .withEndTime("2020-05-24T14:00:01Z")
                        .withDurationInNanos(1_000_000_000L)
                        .build())
                .build();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        codec.writeEvent(span, outputStream);

        // Since traceId is invalid hex, it will trigger DecoderException
        assertThat(outputStream.toByteArray()).isEmpty(); // nothing written
    }

    @Test
    void testWriteEvent_withNonSpanEvent_throwsException() {
        JacksonEvent fakeEvent = JacksonEvent.builder()
                .withEventType("fake")
                .withData(Map.of("key", "value"))
                .build();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        assertThatThrownBy(() -> codec.writeEvent(fakeEvent, outputStream))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("only supports Span");
    }

}
