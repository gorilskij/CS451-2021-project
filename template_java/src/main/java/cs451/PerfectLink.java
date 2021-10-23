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

class SendThread extends Thread {
    private final int SENDING_BATCH_SIZE = 100;

    private final DatagramSocket socket;

    private final HashMap<Integer, DatagramPacket> waitingPackets = new HashMap<>();
    private final HashMap<Integer, DatagramPacket> sendingPackets = new HashMap<>();
    // ids of packets that have been sent and successfully received
    private final HashSet<Integer> removed = new HashSet<>();

    private final HashMap<FullAddress, SendQueue> sendQueues;

    public SendThread(DatagramSocket socket, HashMap<FullAddress, SendQueue> sendQueues) {
        this.socket = socket;
        this.sendQueues = sendQueues;
    }

    public void send(int packetId, DatagramPacket packet) {
        synchronized (sendingPackets) {
            if (sendingPackets.size() < SENDING_BATCH_SIZE) {
                try {
                    socket.send(packet);
                } catch (IOException ignore) {
                }
                sendingPackets.put(packetId, packet);
            } else {
                synchronized (waitingPackets) {
                    waitingPackets.put(packetId, packet);
                }
            }
        }
    }

    public void remove(int packetId) {
        System.out.println("call remove for " + packetId);
        synchronized (sendingPackets) {
            if (removed.add(packetId)) {
                System.out.println("SUCCESSFULLY REMOVE PACKET " + packetId);
                DatagramPacket removedPacket = sendingPackets.remove(packetId);
                if (removedPacket == null) {
                    throw new IllegalStateException(
                            "sender received a remove command for a message that has never existed: " + packetId
                    );
                }

                // add a message from allMessages to sendingBatch and send it in the process
                synchronized (waitingPackets) {
                    Optional<Integer> firstKey = waitingPackets.keySet().stream().findFirst();
                    if (firstKey.isPresent()) {
                        DatagramPacket newPacket = waitingPackets.remove(firstKey.get());
                        try {
                            socket.send(newPacket);
                        } catch (IOException ignored) {
                        }
                        sendingPackets.put(firstKey.get(), newPacket);
                        System.out.println("sent packet " + firstKey.get() + " due to ack received for " + packetId);
                    }
                }
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                interrupt();
            }

            long nanoTime = System.nanoTime();
            synchronized (sendQueues) {
                for (SendQueue queue : sendQueues.values()) {
                    queue.ping(nanoTime);
                }
            }

            if (isInterrupted()) {
                synchronized (waitingPackets) {
                    synchronized (sendingPackets) {
                        if (waitingPackets.isEmpty() && sendingPackets.isEmpty()) {
                            break;
                        }
                    }
                }
            }

            System.out.println("henlou");
            synchronized (sendingPackets) {
//                for (DatagramPacket packet : sendingPackets.values()) {
                for (Map.Entry<Integer, DatagramPacket> entry : sendingPackets.entrySet()) {
                    int packetId = entry.getKey();
                    DatagramPacket packet = entry.getValue();

                    // TODO: maybe send in bursts? like 5 copies at a time?
                    // TODO: consider batching messages together
                    try {
                        socket.send(packet);
                        System.out.println("send packet " + packetId);
                    } catch (IOException ignored) {
                    }
                    System.out.println("!! sent repeat");
                }
            }
        }
    }
}

class ReceiveThread extends Thread {
    private final DatagramSocket socket;
    private final byte[] buffer = new byte[Constants.MAX_PACKET_SIZE];

    private final Consumer<DatagramPacket> normalPacketCallback;
    private final Consumer<Integer> acknowledgementCallback;

    ReceiveThread(DatagramSocket socket, Consumer<DatagramPacket> normalPacketCallback, Consumer<Integer> acknowledgementCallback) {
        this.socket = socket;
        this.normalPacketCallback = normalPacketCallback;
        this.acknowledgementCallback = acknowledgementCallback;
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            try {
                socket.receive(packet);
            } catch (SocketTimeoutException e) {
                continue;
            } catch (IOException e) {
                throw new Error(e);
            }

            int packetId = BigEndianCoder.decodeInt(buffer, 0);
            if (packetId == 0) {
                // acknowledgement
                int acknowledgedPacketId = BigEndianCoder.decodeInt(buffer, 8);
                acknowledgementCallback.accept(acknowledgedPacketId);
            } else {
                // normal packet
                normalPacketCallback.accept(packet);
            }
        }
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

        sendThread = new SendThread(socket, sendQueues);

        receiveThread = new ReceiveThread(
                socket,
                normalPacket -> {
                    byte[] packetData = normalPacket.getData();
                    int packetId = BigEndianCoder.decodeInt(packetData, 0);
                    int sourceId = BigEndianCoder.decodeInt(packetData, 4);
                    PacketKey key = new PacketKey(packetId, sourceId);
                    if (receivedIds.contains(key)) {
                        Packet acknowledgement = new Packet(packetId, packetData).acknowledgement(sourceId);
                        try {
                            socket.send(new DatagramPacket(acknowledgement.data, acknowledgement.data.length, normalPacket.getAddress(), normalPacket.getPort()));
                        } catch (IOException ignored) {
                        }
                    } else {
                        receivedIds.add(key);
                        synchronized (receiveQueue) {
                            receiveQueue.add(normalPacket);
                        }
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
