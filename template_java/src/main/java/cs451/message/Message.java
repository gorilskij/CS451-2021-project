package cs451.message;

// Note that PLMessage and URBMessage are internally the same but semantically different
public abstract class Message {
    final int messageId;
    final int sourceId;
    private String text = null;
    private byte[] textBytes = null;

    public Message(int messageId, int sourceId, String text) {
        this.messageId = messageId;
        this.sourceId = sourceId;
        this.text = text;
    }

    public Message(int messageId, int sourceId, byte[] textBytes) {
        this.messageId = messageId;
        this.sourceId = sourceId;
        this.textBytes = textBytes;
    }

    public Message(Message message) {
        this(message.messageId, message.sourceId, message.getTextBytes());
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
