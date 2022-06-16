package nz.co.eroad.hackathon.model;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class GpsData {

    private BigDecimal accuracy;
    private BigDecimal bearing;
    private BigDecimal lat;
    private BigDecimal lon;
    private BigDecimal numStats;
    private BigDecimal speed;
    private BigDecimal timestamp;
}
