package gaiasim.JCTCalc;

public class CCTInfo {

    private final double startTime;
    private final double endTime;
    private final double duration;

    public CCTInfo(double startTime, double endTime, double duration) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.duration = duration;
    }

    public CCTInfo(String startTimeStr, String endTimeStr, String durationStr) {
        this.startTime = Double.parseDouble(startTimeStr);
        this.endTime = Double.parseDouble(endTimeStr);
        this.duration = Double.parseDouble(durationStr);
    }

    public double getStartTime() {
        return startTime;
    }

    public double getEndTime() {
        return endTime;
    }

    public double getDuration() {
        return duration;
    }

}
