package cs451;

public class Message {
    public final int messageId;
    public final String text;
    public final int sourceId;

    Message(int messageId, String text, int sourceId) {
        this.messageId = messageId;
        this.text = text;
        this.sourceId = sourceId;
    }
}
