package cs451;

import java.net.DatagramPacket;
import java.util.ArrayList;
import java.util.Arrays;

class MessageFragment {
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

public class SendQueue {
    private final FullAddress destination;
    private final int sourceId;
    private final SendThread sendThread;

    // TODO: have a running counter of the total length of the fragments in the queue
    private final ArrayList<MessageFragment> queue = new ArrayList<>();
    private int totalQueueSize = 0;

    private int nextPacketId = 1;
    private long lastSendNanoTime = 0;

    public SendQueue(FullAddress destination, int sourceId, SendThread sendThread) {
        this.destination = destination;
        this.sourceId = sourceId;
        this.sendThread = sendThread;
    }

    public void send(int messageId, String text) {
        synchronized (this) {
            MessageFragment fragment = new MessageFragment(messageId, text);
            queue.add(fragment);
            totalQueueSize += fragment.size();

            Packet maybePacket = tryMakePacket();
            if (maybePacket != null) {
                DatagramPacket packet = new DatagramPacket(maybePacket.data, maybePacket.data.length, destination.address, destination.port);
                sendThread.send(maybePacket.packetId, packet);
            }

            if (queue.isEmpty()) {
                System.out.println("SEND QUEUE EMPTY FOR " + sourceId);
            }
        }
    }

    private void flush() {
        Packet maybePacket = forceMakePacket();
        if (maybePacket != null) {
            DatagramPacket packet = new DatagramPacket(maybePacket.data, maybePacket.data.length, destination.address, destination.port);
            sendThread.send(maybePacket.packetId, packet);
        }

        System.out.println("SEND QUEUE EMPTY FOR " + sourceId);
    }

    public void ping(long nanoTime) {
        synchronized (this) {
            if (nanoTime - lastSendNanoTime >= 10_000_000) {
                flush();
                lastSendNanoTime = nanoTime;
            }
        }
    }

    // makes a packet even if it would be underfilled
    // only returns null if there are no fragments at all
    public Packet forceMakePacket() {
        synchronized (this) {
            if (queue.isEmpty()) {
                return null;
            }

            ArrayList<MessageFragment> fragments = new ArrayList<>();
            int totalLength = 0;
            do {
                int spaceRemaining = Constants.MAX_PACKET_SIZE - totalLength - 8;
                if (spaceRemaining <= MessageFragment.METADATA_LENGTH) {
                    break;
                }

                if (queue.get(0).size() <= spaceRemaining) {
                    MessageFragment fragment = queue.remove(0);
                    totalLength += fragment.size();
                    fragments.add(fragment);
                } else {
                    MessageFragment[] halves = queue.get(0).split(spaceRemaining);
                    totalLength += halves[0].size();
                    fragments.add(halves[0]);
                    queue.set(0, halves[1]);
                }
            } while (!queue.isEmpty());

            byte[] packetBytes = new byte[8 + totalLength];
            int packetId = nextPacketId;
            nextPacketId++;
            BigEndianCoder.encodeInt(packetId, packetBytes, 0);
            BigEndianCoder.encodeInt(sourceId, packetBytes, 4);

            int currentIdx = 8;
            for (MessageFragment fragment : fragments) {
                byte[] fragmentBytes = fragment.getBytes();
                System.arraycopy(fragmentBytes, 0, packetBytes, currentIdx, fragmentBytes.length);
                currentIdx += fragmentBytes.length;
            }

            return new Packet(packetId, packetBytes);
        }
    }

    // only makes a packet if there are enough fragments for a full one,
    // otherwise returns null
    public synchronized Packet tryMakePacket() {
        if (totalQueueSize >= Constants.MAX_PACKET_SIZE) {
            return forceMakePacket();
        }

        return null;
    }
}
