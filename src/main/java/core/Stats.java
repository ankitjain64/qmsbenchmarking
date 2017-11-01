package core;

/**
 * Created By Maharia
 */
public class Stats {
    /**
     * Time of start of the experiment
     */
    private Long startTime;
    /**
     * Time of end of the expermient
     */
    private Long endTime;
    /**
     * Number of messages sent
     */
    private Long sendCount;
    /**
     * Number of messages rcvd
     */
    private Long rcvCount;

    public Stats(Long startTime) {
        this.startTime = startTime;
        this.endTime = null;
        this.sendCount = 0L;
        this.rcvCount = 0L;
    }

    public Stats(Stats other, long endTime) {
        this.startTime = other.startTime;
        this.endTime = endTime;
        this.sendCount = other.sendCount;
        this.rcvCount = other.rcvCount;
    }

    public void incrementSendCount() {
        this.sendCount++;
    }

    public void incrementRcvCount() {
        this.rcvCount++;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public Long getStartTime() {
        return startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public Long getSendCount() {
        return sendCount;
    }

    public Long getRcvCount() {
        return rcvCount;
    }

    @Override
    public String toString() {
        return "Stats{" +
                "startTime=" + startTime +
                ", endTime=" + endTime +
                ", sendCount=" + sendCount +
                ", rcvCount=" + rcvCount +
                '}';
    }
}
