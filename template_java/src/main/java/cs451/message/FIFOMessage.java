package cs451.message;

public class FIFOMessage extends Message {
    public FIFOMessage(int fifoMessageId, int fifoSourceId, String text) {
        super(fifoMessageId, fifoSourceId, text);
    }

    public FIFOMessage(int fifoMessageId, int fifoSourceId, byte[] textBytes) {
        super(fifoMessageId, fifoSourceId, textBytes);
    }

    public FIFOMessage(URBMessage urbMessage) {
        super(urbMessage);
    }

    public int getFifoMessageId() {
        return messageId;
    }

    public int getFifoSourceId() {
        return sourceId;
    }
}
