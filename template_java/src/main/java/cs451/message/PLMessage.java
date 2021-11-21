package cs451.message;

public class PLMessage extends Message {
    public PLMessage(int plMessageId, int plSourceId, String text) {
        super(plMessageId, plSourceId, text);
    }

    public PLMessage(int plMessageId, int plSourceId, byte[] textBytes) {
        super(plMessageId, plSourceId, textBytes);
    }

    public int getPlMessageId() {
        return messageId;
    }

    public int getPlSourceId() {
        return sourceId;
    }
}
