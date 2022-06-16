package nz.co.eroad.hackathon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import nz.co.eroad.hackathon.model.GpsTelemetry;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.utils.StringInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

@Slf4j
public class Handler implements RequestHandler<Object, Context> {

    private static final Gson GSON = new Gson();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final S3Client s3Client = S3Client.builder().region(Region.AP_SOUTHEAST_2).build();

    @Override
    public Context handleRequest(Object event, Context context) {
        String json = Try.of(() -> objectMapper.writeValueAsString(event)).get();
        ObjectNode objectNode = Try.of(() -> objectMapper.readValue(json, ObjectNode.class)).get();
        JsonNode records = objectNode.path("Records");

        records.elements()
                .forEachRemaining(jsonNode -> {
                    JsonNode s3 = jsonNode.path("s3");
                    JsonNode bucket = s3.path("bucket");
                    JsonNode bucketName = bucket.path("name");
                    JsonNode object = s3.path("object");
                    JsonNode key = object.path("key");

                    try (StringWriter stringWriter = new StringWriter();
                         InputStream inputStream = new StringInputStream(stringWriter.toString())) {
                        ResponseInputStream<GetObjectResponse> response = s3Client.getObject(
                                GetObjectRequest.builder().bucket(bucketName.asText()).key(key.asText()).build()
                        );

                        process(response, stringWriter);

                        s3Client.putObject(
                                PutObjectRequest.builder()
                                        .bucket(bucketName.asText())
                                        .key(key.asText().replace(".gps", ".csv"))
                                        .build(),
                                RequestBody.fromInputStream(inputStream, stringWriter.toString().length())
                        );
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });

        return null;
    }

    private void process(InputStream inputStream, StringWriter stringWriter) throws IOException {
        try (Reader reader = new InputStreamReader(inputStream); CSVPrinter csvPrinter = CSVFormat.DEFAULT.print(stringWriter)) {
            GpsTelemetry gpsTelemetry = GSON.fromJson(reader, GpsTelemetry.class);
            Instant start = Instant.ofEpochMilli(gpsTelemetry.getStart().longValue());

            csvPrinter.printRecord("time (ms)", "bearing (deg)", "lat (deg)", "lon (deg)", "alt (m)", "speed (m/s)");
            gpsTelemetry.getLogs()
                    .forEach(gpsData ->{
                        Instant timestamp = Instant.ofEpochMilli(gpsData.getTimestamp().longValue());
                        long time = Duration.between(start, timestamp).toMillis();
                        BigDecimal bearing = gpsData.getBearing();
                        BigDecimal latitude = gpsData.getLat();
                        BigDecimal longitude = gpsData.getLon();
                        BigDecimal speed = gpsData.getSpeed();
                        BigDecimal altitude = BigDecimal.ZERO;

                        Try.run(() -> csvPrinter.printRecord(time, bearing, latitude, longitude, altitude, speed));
                    });
        }
    }
}
