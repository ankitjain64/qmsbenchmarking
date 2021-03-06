package core;

@SuppressWarnings("WeakerAccess")
public class Message {
    /**
     * Message number
     */
    private long num;
    /**
     * Text of the message
     */
    private String text;
    /**
     * Id of the producer who generated it
     */
    private int pId;
    /**
     * Id of the consumer who rcvd it
     */
    private int cId;
    /**
     * Producer generated time stamp
     */
    private Long pTs;

    /**
     * Consumer Rcvd Time Stamp
     */
    private Long cTs;

    /**
     * We need to check order for the key which have same values
     * TODO: Might need to work for it
     */
    private String orderKey;

    public Message(int pId, Long pTs, Long messageNumber) {
        this.pId = pId;
        this.pTs = pTs;
        this.num = messageNumber;
    }

    public long getNum() {
        return num;
    }

    public void setNum(long num) {
        this.num = num;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public int getpId() {
        return pId;
    }

    public void setpId(int pId) {
        this.pId = pId;
    }

    public int getcId() {
        return cId;
    }

    public void setcId(int cId) {
        this.cId = cId;
    }

    public Long getpTs() {
        return pTs;
    }

    public void setpTs(Long pTs) {
        this.pTs = pTs;
    }

    public Long getcTs() {
        return cTs;
    }

    public void setcTs(Long cTs) {
        this.cTs = cTs;
    }

    public String getOrderKey() {
        return orderKey;
    }

    public void setOrderKey(String orderKey) {
        this.orderKey = orderKey;
    }

    public String constructMessageId() {
        return this.pId + "." + this.num;
    }

    public Long getDelta() {
        return this.cTs - this.pTs;
    }

    @Override
    public String toString() {
        return "Message{" +
                "text='" + text + '\'' +
                ", pTs=" + pTs +
                ", cTs=" + cTs +
                ", orderKey='" + orderKey + '\'' +
                '}';
    }
}
