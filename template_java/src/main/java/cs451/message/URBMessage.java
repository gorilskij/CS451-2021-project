package cs451.message;

public class URBMessage extends Message {
    public URBMessage(int urbMessageId, int urbSourceId, String text) {
        super(urbMessageId, urbSourceId, text);
    }

    public URBMessage(int urbMessageId, int urbSourceId, byte[] textBytes) {
        super(urbMessageId, urbSourceId, textBytes);
    }

    public URBMessage(URBMessage message) {
        super(message);
    }

    public int getUrbMessageId() {
        return messageId;
    }

    public int getUrbSourceId() {
        return sourceId;
    }
}
