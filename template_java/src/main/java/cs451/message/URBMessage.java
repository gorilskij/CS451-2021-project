package cs451.message;

public class URBMessage extends Message {
    public URBMessage(int messageId, int sourceId, String text) {
        super(messageId, sourceId, text);
    }

    public URBMessage(int messageId, int sourceId, byte[] textBytes) {
        super(messageId, sourceId, textBytes);
    }
}
