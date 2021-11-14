package cs451.message;

public class FIFOMessage extends URBMessage {
    public FIFOMessage(int messageId, int sourceId, String text) {
        super(messageId, sourceId, text);
    }

    public FIFOMessage(int messageId, int sourceId, byte[] textBytes) {
        super(messageId, sourceId, textBytes);
    }

    public FIFOMessage(URBMessage urbMessage) {
        super(urbMessage);
    }
}
