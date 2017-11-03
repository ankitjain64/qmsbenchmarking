package core;

/**
 * Created By Maharia
 */
@SuppressWarnings("WeakerAccess")
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

    /**
     * Failed count of the messages
     */
    private Long failedCount;

    /**
     * Total acked Messages
     */
    private Long ackCount;

    public Stats(Long startTime) {
        this.startTime = startTime;
        this.endTime = null;
        this.sendCount = 0L;
        this.rcvCount = 0L;
        this.failedCount = 0L;
        this.ackCount = 0L;
    }

    public Stats createSnapShot(long endTime) {
        synchronized (this) {
            Stats stats = new Stats(this.startTime);
            stats.endTime = endTime;
            stats.sendCount = this.sendCount;
            stats.rcvCount = this.rcvCount;
            return stats;
        }
    }

    public void incrementSendCount() {
        this.sendCount++;
    }

    public void incrementRcvCount() {
        this.rcvCount++;
    }

    public void incrementFailCount() {
        this.failedCount++;
    }

    public void incrementAckCount() {
        this.ackCount++;
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
