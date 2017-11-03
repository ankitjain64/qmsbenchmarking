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

    private boolean isOutofOrder;

    public Stats(Long startTime) {
        this.startTime = startTime;
        this.endTime = null;
        this.sendCount = 0L;
        this.rcvCount = 0L;
        this.failedCount = 0L;
        this.ackCount = 0L;
        this.isOutofOrder = false;
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

    public boolean isOutofOrder() {
        return isOutofOrder;
    }

    public void setOutofOrder(boolean outofOrder) {
        isOutofOrder = outofOrder;
    }

    public static String getCsvHeaders() {
        StringBuilder sb = new StringBuilder();
        sb.append("Start Time").append(",");
        sb.append("End Time").append(",");
        sb.append("Send Count").append(",");
        sb.append("Rcv Count").append(",");
        sb.append("Fail Count").append(",");
        sb.append("Ack Count").append(",");
        sb.append("Out of order");
        return sb.toString();
    }

    public String getRowValues() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.startTime).append(",");
        sb.append(this.endTime).append(",");
        sb.append(this.sendCount).append(",");
        sb.append(this.rcvCount).append(",");
        sb.append(this.failedCount).append(",");
        sb.append(this.ackCount).append(",");
        sb.append(this.isOutofOrder);
        return sb.toString();
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
