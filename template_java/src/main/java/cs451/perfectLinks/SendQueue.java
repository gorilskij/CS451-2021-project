package cs451.perfectLinks;

import cs451.BigEndianCoder;
import cs451.Constants;
import cs451.FullAddress;

import java.net.DatagramPacket;
import java.util.ArrayList;

public class SendQueue {
    private final FullAddress destination;
    private final int sourceId;
    private final SendThread sendThread;

    // TODO: have a running counter of the total length of the fragments in the queue
    private final ArrayList<MessageFragment> queue = new ArrayList<>();
    private int totalQueueSize = 0;

    private int nextPacketId = 1;

    public SendQueue(FullAddress destination, int sourceId, SendThread sendThread) {
        this.destination = destination;
        this.sourceId = sourceId;
        this.sendThread = sendThread;
    }

    public void send(int messageId, String text) {
        MessageFragment fragment = new MessageFragment(messageId, text);
        queue.add(fragment);
        totalQueueSize += fragment.size();

        Packet maybePacket = tryMakePacket();
        if (maybePacket != null) {
            DatagramPacket packet = new DatagramPacket(maybePacket.data, maybePacket.data.length, destination.address, destination.port);
            sendThread.send(maybePacket.packetId, packet);
        }
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
    public Packet forceMakePacket() {
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

    // only makes a packet if there are enough fragments for a full one,
    // otherwise returns null
    public Packet tryMakePacket() {
        if (totalQueueSize >= Constants.MAX_PACKET_SIZE) {
            return forceMakePacket();
        }

        return null;
    }
}
