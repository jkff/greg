package org.greg.server;

public class Configuration {
    public int messagePort;
    public int calibrationPort;
    public int preCalibrationIters;
    public int minCalibrationIters;
    public int maxCalibrationIters;
    public double desiredConfidenceRangeMs;
    public double desiredConfidenceLevel;
    public int maxPendingUncalibrated;
    public int maxPendingCalibrated;
    public int timeWindowSec;
}
