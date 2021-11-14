package cs451.message;

public class PLMessage extends Message {
    public PLMessage(int messageId, int sourceId, String text) {
        super(messageId, sourceId, text);
    }

    public PLMessage(int messageId, int sourceId, byte[] textBytes) {
        super(messageId, sourceId, textBytes);
    }
}
