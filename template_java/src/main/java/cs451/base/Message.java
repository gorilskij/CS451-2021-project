package cs451.base;

public class Message {
    public final int messageId;
    public final String text;
    public final int sourceId;

    public Message(int messageId, String text, int sourceId) {
        this.messageId = messageId;
        this.text = text;
        this.sourceId = sourceId;
    }
}
