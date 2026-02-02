package org.rancidcode.telemetryaggregator.config;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

public class AvgStats {

    private int count;
    private double sumTemperature;
    private double sumHumidity;
    private double sumHeatIndex;
    private String startTime;
    private String endTime;

    public AvgStats() {
    }

    private void add(double temperature, double humidity, double heatIndex, String timeStamp) {

        if (count == 0) {
            startTime = timeStamp;
        }
        endTime = timeStamp;

        sumTemperature += temperature;
        sumHumidity += humidity;
        sumHeatIndex += heatIndex;

        count += 1;
    }

    private double avg(double value) {
        return count == 0 ? 0.0 : value / count;
    }

    public double getAvgTemperature() {
        return avg(sumTemperature);
    }

    public double getAvgHeatIndex() {
        return avg(sumHeatIndex);
    }

    public double getAvgHumidity() {
        return avg(sumHumidity);
    }

    public int getCount() {
        return count;
    }

    public String getStartTime() {
        return startTime;
    }

    public double getSumTemperature() {
        return sumTemperature;
    }

    public double getSumHumidity() {
        return sumHumidity;
    }

    public double getSumHeatIndex() {
        return sumHeatIndex;
    }

    public String getEndTime() {
        return endTime;
    }

    public void parseJSON(String json) {
        try {
            JsonNode root = new ObjectMapper().readTree(json);
            JsonNode nodeTemperature = root.get("temperature");
            JsonNode nodeHumidity = root.get("humidity");
            JsonNode nodeHeatIndex = root.get("heatIndex");
            JsonNode nodeTime = root.get("timestamp");

            this.add(nodeTemperature.asDouble(), nodeHumidity.asDouble(), nodeHeatIndex.asDouble(), nodeTime.asText());
        } catch (Exception e) {

        }
    }
}