package cs451.perfect_links;

import cs451.base.BigEndianCoder;
import cs451.Constants;
import cs451.base.FullAddress;

import java.net.DatagramPacket;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class SendQueue {
    private final FullAddress destination;
    private final int sourceId;
    private final SendThread sendThread;

    private final Deque<MessageFragment> queue = new LinkedBlockingDeque<>();
    private int totalQueueSize = 0;

    private final Queue<Integer> ackQueue = new LinkedBlockingQueue<>();

    private int nextPacketId = 1; // 0 is used to identify ack packets

    public SendQueue(FullAddress destination, int sourceId, SendThread sendThread) {
        if (destination == null) {
            throw new IllegalArgumentException("destination is null");
        }

        this.destination = destination;
        this.sourceId = sourceId;
        this.sendThread = sendThread;
    }

    private synchronized void sendMessageFragment(MessageFragment fragment) {
        queue.offer(fragment);
        totalQueueSize += fragment.size();

//        testQueue();

        Packet maybePacket = tryMakePacket();
        if (maybePacket != null) {
            sendThread.sendPacket(maybePacket, destination);
        }
    }

    public void send(int messageId, String text) {
        sendMessageFragment(new MessageFragment(messageId, text));
    }

    public void send(int messageId, byte[] textBytes) {
        sendMessageFragment(new MessageFragment(messageId, textBytes));
    }

    public void sendAck(int packetId) {
        ackQueue.offer(packetId);
        Packet maybePacket = tryMakeAckPacket();
        if (maybePacket != null) {
            sendThread.sendAckPacket(maybePacket, destination);
        }
    }

    private void flush() {
        Packet maybePacket;
        while ((maybePacket = forceMakeAckPacket()) != null) {
            sendThread.sendAckPacket(maybePacket, destination);
        }
        while ((maybePacket = forceMakePacket()) != null) {
            sendThread.sendPacket(maybePacket, destination);
        }
    }

    public void awaken() {
        flush();
    }

//    private void testQueue() {
//        int realQueueSize = 0;
//        for (MessageFragment f : queue) {
//            realQueueSize += f.size();
//        }
//        if (realQueueSize != totalQueueSize) {
//            throw new IllegalStateException("real: " + realQueueSize + ", total: " + totalQueueSize);
//        }
//    }

    // makes a packet even if it would be underfilled
    // only returns null if there are no fragments at all
    private synchronized Packet forceMakePacket() {
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

//            System.out.println("space remaining: " + spaceRemaining);
            MessageFragment nextFragment = queue.poll();
            int nextFragmentSize = nextFragment.size();
            totalQueueSize -= nextFragmentSize;
            if (nextFragmentSize <= spaceRemaining) {
                totalLength += nextFragmentSize;
                fragments.add(nextFragment);
            } else {
                MessageFragment[] halves = nextFragment.split(spaceRemaining);
                fragments.add(halves[0]);
                totalLength += halves[0].size();
                queue.offerFirst(halves[1]);
                totalQueueSize += halves[1].size();
//                System.out.println("queue +1= " + halves[1].size());
            }

//            testQueue();
        } while (!queue.isEmpty());

        byte[] packetBytes = new byte[8 + totalLength];
        int packetId = nextPacketId++;
        BigEndianCoder.encodeInt(packetId, packetBytes, 0);
        BigEndianCoder.encodeInt(sourceId, packetBytes, 4);

        int currentIdx = 8;
        for (MessageFragment fragment : fragments) {
            byte[] fragmentBytes = fragment.getBytes();
            System.arraycopy(fragmentBytes, 0, packetBytes, currentIdx, fragmentBytes.length);
            currentIdx += fragmentBytes.length;
        }

        //        System.out.println("packet leaving with " + packetBytes.length + " bytes of payload");
        return new Packet(packetId, packetBytes);
    }

    // only makes a packet if there are enough fragments for a full one,
    // otherwise returns null
    private synchronized Packet tryMakePacket() {
        if (totalQueueSize >= Constants.MAX_PACKET_SIZE) {
            return forceMakePacket();
        }

        return null;
    }

    private synchronized Packet forceMakeAckPacket() {
        if (ackQueue.isEmpty()) {
            return null;
        }

        int takeAcks = Math.min((Constants.MAX_PACKET_SIZE - 8) / 4, ackQueue.size());
        byte[] bytes = new byte[12 + takeAcks * 4];
        BigEndianCoder.encodeInt(0, bytes, 0);
        BigEndianCoder.encodeInt(sourceId, bytes, 4);
        BigEndianCoder.encodeInt(takeAcks, bytes, 8);
        for (int i = 0; i < takeAcks; i += 1) {
            BigEndianCoder.encodeInt(ackQueue.poll(), bytes, i * 4 + 12);
        }
        return new Packet(0, bytes);
    }

    private static final int MIN_ACKS_PER_PACKET = 100;
    private synchronized Packet tryMakeAckPacket() {
        if (ackQueue.size() >= MIN_ACKS_PER_PACKET) {
            return forceMakeAckPacket();
        }
        return null;
    }
}
