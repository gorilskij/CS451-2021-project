package cs451.message;

public class BEBMessage extends Message {
    public BEBMessage(int bebMessageId, int bebSourceId, String text) {
        super(bebMessageId, bebSourceId, text);
    }

    public BEBMessage(int bebMessageId, int bebSourceId, byte[] textBytes) {
        super(bebMessageId, bebSourceId, textBytes);
    }

    public BEBMessage(URBMessage message) {
        super(message);
    }

    public int getBebMessageId() {
        return messageId;
    }

    public int getBebSourceId() {
        return sourceId;
    }
}
