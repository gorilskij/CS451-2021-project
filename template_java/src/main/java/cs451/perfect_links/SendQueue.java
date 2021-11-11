package cs451.perfect_links;

import cs451.base.BigEndianCoder;
import cs451.Constants;
import cs451.base.FullAddress;

import java.net.DatagramPacket;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SendQueue {
    private final FullAddress destination;
    private final int sourceId;
    private final SendThread sendThread;

    private final Deque<MessageFragment> queue = new ConcurrentLinkedDeque<>();
    private int totalQueueSize = 0;

    private final Queue<Integer> ackQueue = new ConcurrentLinkedQueue<>();

    private int nextPacketId = 1; // 0 is used to identify ack packets

    public SendQueue(FullAddress destination, int sourceId, SendThread sendThread) {
        if (destination == null) {
            throw new IllegalArgumentException("destination is null");
        }

        this.destination = destination;
        this.sourceId = sourceId;
        this.sendThread = sendThread;
    }

    private void sendMessageFragment(MessageFragment fragment) {
        if (fragment == null) {
            throw new IllegalStateException("tried to sendMessageFragment(null)");
        }

        queue.add(fragment);
        totalQueueSize += fragment.size();

        Packet maybePacket = tryMakePacket();
        if (maybePacket != null) {
            DatagramPacket packet = new DatagramPacket(maybePacket.data, maybePacket.data.length, destination.address, destination.port);
            sendThread.send(maybePacket.packetId, packet);
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
            DatagramPacket packet = new DatagramPacket(maybePacket.data, maybePacket.data.length, destination.address, destination.port);
            sendThread.send(maybePacket.packetId, packet);
        }
    }

    private void flush() {
        Packet maybePacket;

        while ((maybePacket = forceMakeAckPacket()) != null) {
            DatagramPacket packet = new DatagramPacket(maybePacket.data, maybePacket.data.length, destination.address, destination.port);
            sendThread.send(maybePacket.packetId, packet);
        }

        while ((maybePacket = forceMakePacket()) != null) {
            DatagramPacket packet = new DatagramPacket(maybePacket.data, maybePacket.data.length, destination.address, destination.port);
            sendThread.send(maybePacket.packetId, packet);
        }
    }

    public void awaken() {
        flush();
    }

    // makes a packet even if it would be underfilled
    // only returns null if there are no fragments at all
    private synchronized Packet forceMakePacket() {
        // TODO: resolve this
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
            if (queue.peek().size() <= spaceRemaining) {
                MessageFragment fragment = queue.poll();
                totalLength += fragment.size();
                totalQueueSize -= fragment.size();
                fragments.add(fragment);
            } else {
                MessageFragment[] halves = queue.poll().split(spaceRemaining);
                totalLength += halves[0].size();
                fragments.add(halves[0]);
                queue.offerFirst(halves[1]);
            }
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
