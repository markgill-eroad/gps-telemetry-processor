package nz.co.eroad.hackathon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.google.gson.Gson;
import io.vavr.control.Try;
import nz.co.eroad.hackathon.model.GpsTelemetry;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

public class Handler implements RequestStreamHandler {

    private static final Gson GSON = new Gson();

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        try (Reader reader = new InputStreamReader(inputStream);
             CSVPrinter csvPrinter = CSVFormat.DEFAULT.print(new OutputStreamWriter(outputStream))) {
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
