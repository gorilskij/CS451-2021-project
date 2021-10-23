package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.function.Consumer;

// just a tuple class to uniquely identify each message (message id, sender id)
class PacketKey {
    public final int packetId;
    public final int sourceId;

    PacketKey(int packetId, int sourceId) {
        this.packetId = packetId;
        this.sourceId = sourceId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PacketKey that = (PacketKey) o;
        return packetId == that.packetId && sourceId == that.sourceId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(packetId, sourceId);
    }
}

public class PerfectLink {
    private final int processId;

    // *** sending ***
    private final HashMap<FullAddress, SendQueue> sendQueues = new HashMap<>();
    private final SendThread sendThread;
    private int nextMessageId = 0;

    // *** receiving ***
    private final ReceiveThread receiveThread;
    // TODO: replace with actual queue or stack
    private final ReceiveQueue receiveQueue = new ReceiveQueue();
    private final HashSet<PacketKey> receivedIds = new HashSet<>();

    public PerfectLink(int processId, DatagramSocket socket) {
        this.processId = processId;

        sendThread = new SendThread(socket, () -> {
            synchronized (sendQueues) {
                for (SendQueue queue : sendQueues.values()) {
                    queue.awaken();
                }
            }
        });

        receiveThread = new ReceiveThread(
                socket,
                normalPacket -> {
                    byte[] packetData = normalPacket.getData();
                    int packetId = BigEndianCoder.decodeInt(packetData, 0);
                    int sourceId = BigEndianCoder.decodeInt(packetData, 4);

//                    System.out.println("> packet id " + packetId + " from " + sourceId);

                    PacketKey key = new PacketKey(packetId, sourceId);
                    if (!receivedIds.contains(key)) {
                        receivedIds.add(key);
                        synchronized (receiveQueue) {
                            receiveQueue.add(normalPacket);
                        }
                    }

                    Packet acknowledgement = new Packet(packetId, packetData).acknowledgement(sourceId);
                    try {
                        socket.send(new DatagramPacket(acknowledgement.data, acknowledgement.data.length, normalPacket.getAddress(), normalPacket.getPort()));
                    } catch (IOException ignored) {
                    }
                },
                sendThread::remove
        );

        sendThread.start();
        receiveThread.start();
    }

    public void send(String msg, FullAddress destination) {
        System.out.println("Enqueue \"" + msg + "\" to " + destination);

        int messageId = nextMessageId++;

        synchronized (sendQueues) {
            if (!sendQueues.containsKey(destination)) {
                sendQueues.put(destination, new SendQueue(destination, processId, sendThread));
            }

            sendQueues.get(destination).send(messageId, msg);
        }
    }

    // non-blocking, returns null if there is nothing to deliver at the moment
    public Message tryDeliver() {
        synchronized (receiveQueue) {
            return receiveQueue.tryDeliver();
        }
    }

    public void close() {
        sendThread.interrupt();
        receiveThread.interrupt();

        try {
            sendThread.join();
        } catch (InterruptedException ignore) {
        }

        try {
            receiveThread.join();
        } catch (InterruptedException ignore) {
        }
    }
}
