package nz.co.eroad.hackathon.model;

import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

@Data
public class GpsTelemetry {

    private BigDecimal start;
    private BigDecimal end;
    private List<GpsData> logs;
}
