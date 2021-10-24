package cs451.perfectLinks;

import cs451.BigEndianCoder;

public class MessageFragment {
    static final int METADATA_LENGTH = 13;

    // metadata
    public final int messageId; // 4 bytes
    public final int fragmentIdx; // 4 bytes
    public final boolean isLast; // 1 byte
    // data length // 4 bytes

    public final byte[] data; // does not contain metadata

    MessageFragment(int messageId, int fragmentIdx, boolean isLast, byte[] data) {
        this.messageId = messageId;
        this.fragmentIdx = fragmentIdx;
        this.isLast = isLast;
        this.data = data;
    }

    MessageFragment(int messageId, String text) {
        this(messageId, 0, true, text.getBytes());
    }

    // extracts the first message from a byte array
    MessageFragment(byte[] packetBody, int startIndex) {
        if (packetBody.length - startIndex < METADATA_LENGTH + 1) {
            throw new IllegalStateException("packetBody too short");
        }

        messageId = BigEndianCoder.decodeInt(packetBody, startIndex);
        fragmentIdx = BigEndianCoder.decodeInt(packetBody, startIndex + 4);
        byte isLast = packetBody[startIndex + 8];
        switch (isLast) {
            case 0:
                this.isLast = false;
                break;
            case 1:
                this.isLast = true;
                break;
            default:
                throw new IllegalStateException("invalid boolean byte: " + isLast);
        }

        int dataLength = BigEndianCoder.decodeInt(packetBody, startIndex + 9);
        data = new byte[dataLength];
        System.arraycopy(packetBody, startIndex + METADATA_LENGTH, data, 0, dataLength);
    }

    public int size() {
        return METADATA_LENGTH + data.length;
    }

    byte[] getBytes() {
        byte[] bytes = new byte[METADATA_LENGTH + data.length];
        BigEndianCoder.encodeInt(messageId, bytes, 0);
        BigEndianCoder.encodeInt(fragmentIdx, bytes, 4);
        bytes[8] = (byte) (isLast ? 1 : 0);
        BigEndianCoder.encodeInt(data.length, bytes, 9);
        System.arraycopy(data, 0, bytes, METADATA_LENGTH, data.length);
        return bytes;
    }

    MessageFragment[] split(int firstHalfSize) {
        if (firstHalfSize <= METADATA_LENGTH) {
            throw new IllegalArgumentException("size must be > " + METADATA_LENGTH);
        }

        if (firstHalfSize - METADATA_LENGTH >= data.length) {
            return new MessageFragment[] {this, null};
        }

        byte[] firstHalfBytes = new byte[firstHalfSize - METADATA_LENGTH];
        System.arraycopy(data, 0, firstHalfBytes, 0, firstHalfBytes.length);
        MessageFragment firstHalf = new MessageFragment(messageId, fragmentIdx, false, firstHalfBytes);

        byte[] secondHalfBytes = new byte[data.length - firstHalfBytes.length];
        System.arraycopy(data, firstHalfBytes.length, secondHalfBytes, 0, secondHalfBytes.length);
        MessageFragment secondHalf = new MessageFragment(messageId, fragmentIdx + 1, true, secondHalfBytes);

        return new MessageFragment[] {firstHalf, secondHalf};
    }
}
