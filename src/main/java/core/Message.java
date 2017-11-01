package core;

public class Message {

    private long num;

    private String text;

    private int pId;

    private int cId;

    private Long pTs;

    private Long cTs;

    /**
     * We need to check order for the key which have same values
     */
    private String orderKey;

    public Message(int pId, Long pTs, Long messageNumber) {
        this.pId = pId;
        this.pTs = pTs;
        this.num = messageNumber;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
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
