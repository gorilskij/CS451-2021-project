package cs451.perfect_links;

import cs451.base.BigEndianCoder;
import cs451.Constants;
import cs451.base.FullAddress;

import java.util.ArrayList;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class SendQueue {
    private final int sourceId;
    private final int destinationId;
    private final SendThread sendThread;

    private final BlockingDeque<MessageFragment> queue = new LinkedBlockingDeque<>();
    private int totalQueueSize = 0;

    private final BlockingQueue<Integer> ackQueue = new LinkedBlockingQueue<>();

    private int nextPacketId = 1; // 0 is used to identify ack packets

    public SendQueue(int sourceId, int destinationId, SendThread sendThread) {
        this.sourceId = sourceId;
        this.destinationId = destinationId;
        this.sendThread = sendThread;
    }

    private synchronized void sendMessageFragment(MessageFragment fragment) {
        try {
            queue.put(fragment);
        } catch (InterruptedException ignored) {
        }
        totalQueueSize += fragment.size();

//        testQueue();

        Packet maybePacket = tryMakePacket();
        if (maybePacket != null) {
            sendThread.sendPacket(maybePacket, destinationId);
        }
    }

    public void send(int messageId, byte[] textBytes) {
        sendMessageFragment(new MessageFragment(messageId, textBytes));
    }

    public void sendAck(int packetId) {
        try {
            ackQueue.put(packetId);
        } catch (InterruptedException ignored) {
        }
        Packet maybePacket = tryMakeAckPacket();
        if (maybePacket != null) {
            sendThread.sendPacket(maybePacket, destinationId);
        }
    }

    public void flush() {
        Packet maybePacket;
        while ((maybePacket = forceMakeAckPacket()) != null) {
            sendThread.sendPacket(maybePacket, destinationId);
        }
        while ((maybePacket = forceMakePacket()) != null) {
            sendThread.sendPacket(maybePacket, destinationId);
        }
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
            MessageFragment nextFragment = null;
            try {
                nextFragment = queue.take();
            } catch (InterruptedException ignored) {
            }
            int nextFragmentSize = nextFragment.size();
            totalQueueSize -= nextFragmentSize;
            if (nextFragmentSize <= spaceRemaining) {
                totalLength += nextFragmentSize;
                fragments.add(nextFragment);
            } else {
                MessageFragment[] halves = nextFragment.split(spaceRemaining);
                fragments.add(halves[0]);
                totalLength += halves[0].size();
                try {
                    queue.putFirst(halves[1]);
                } catch (InterruptedException ignored) {
                }
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

        return new Packet(packetBytes);
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
            try {
                BigEndianCoder.encodeInt(ackQueue.take(), bytes, i * 4 + 12);
            } catch (InterruptedException ignored) {
            }
        }
        return new Packet(bytes);
    }

    private synchronized Packet tryMakeAckPacket() {
        if (ackQueue.size() >= Constants.PL_MIN_ACKS_PER_PACKET) {
            return forceMakeAckPacket();
        }
        return null;
    }
}
