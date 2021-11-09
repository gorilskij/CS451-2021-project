package cs451.perfect_links;

import cs451.base.BigEndianCoder;
import cs451.Constants;
import cs451.base.FullAddress;

import java.net.DatagramPacket;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

public class SendQueue {
    private final FullAddress destination;
    private final int sourceId;
    private final SendThread sendThread;

    // TODO: have a running counter of the total length of the fragments in the queue
    private final Deque<MessageFragment> queue = new ConcurrentLinkedDeque<>();
    private int totalQueueSize = 0;

    private int nextPacketId = 1;

    public SendQueue(FullAddress destination, int sourceId, SendThread sendThread) {
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

    private void flush() {
        Packet maybePacket;
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
    public synchronized Packet forceMakePacket() {
        // TODO: resolve this
        synchronized (queue) {
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

                MessageFragment peek = queue.peek();
                if (peek == null) {
                    System.out.println("NULL PEEK");
                    System.out.println("queue size: " + queue.size());
                    System.out.println(":: " + queue);
                    MessageFragment fragment = queue.poll();
                    System.out.println("polled: " + fragment);
                    System.out.println("> size: " + fragment.size());
                    System.exit(1);
                }
                if (peek.size() <= spaceRemaining) {
                    MessageFragment fragment = queue.poll();
                    totalLength += fragment.size();
                    fragments.add(fragment);
                } else {
                    MessageFragment[] halves = queue.poll().split(spaceRemaining);
                    totalLength += halves[0].size();
                    fragments.add(halves[0]);
                    queue.offerFirst(halves[1]);
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
    public Packet tryMakePacket() {
        if (totalQueueSize >= Constants.MAX_PACKET_SIZE) {
            return forceMakePacket();
        }

        return null;
    }
}
