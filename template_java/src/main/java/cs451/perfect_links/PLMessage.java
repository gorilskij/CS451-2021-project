package cs451.perfect_links;

public class PLMessage {
    public final int messageId;
    public final int sourceId;
    private String text = null;
    private byte[] textBytes = null;

    public PLMessage(int messageId, int sourceId, String text) {
        this.messageId = messageId;
        this.sourceId = sourceId;
        this.text = text;
    }

    public PLMessage(int messageId, int sourceId, byte[] textBytes) {
        this.messageId = messageId;
        this.sourceId = sourceId;
        this.textBytes = textBytes;
    }

    public String getText() {
        if (text == null) {
            text = new String(textBytes);
        }
        return text;
    }

    public byte[] getTextBytes() {
        if (textBytes == null) {
            textBytes = text.getBytes();
        }
        return textBytes;
    }
}
