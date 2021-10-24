package cs451.perfectLinks;

import cs451.Pair;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SendThread extends Thread {
    private final int SENDING_BATCH_SIZE = 100;

    private final DatagramSocket socket;

    // contains (packetId, packet)
    private final ConcurrentLinkedQueue<Pair<Integer, DatagramPacket>> waitingPackets = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<Integer, DatagramPacket> sendingPackets = new ConcurrentHashMap<>(SENDING_BATCH_SIZE);
    // ids of packets that have been sent and successfully received
    private final HashSet<Integer> removed = new HashSet<>();

    private final Runnable awakenSenders;

    public SendThread(DatagramSocket socket, Runnable awakenSenders) {
        this.socket = socket;
        this.awakenSenders = awakenSenders;
    }

    public void send(int packetId, DatagramPacket packet) {
        if (sendingPackets.size() < SENDING_BATCH_SIZE) {
            try {
                socket.send(packet);
            } catch (IOException ignore) {
            }
            sendingPackets.put(packetId, packet);
        } else {
            waitingPackets.offer(new Pair<>(packetId, packet));
        }
    }

    public void remove(int packetId) {
        if (removed.add(packetId)) {
            DatagramPacket removedPacket = sendingPackets.remove(packetId);
            if (removedPacket == null) {
                throw new IllegalStateException(
                        "sender received a remove command for a message that has never existed: " + packetId
                );
            }

            // add a message from allMessages to sendingBatch and send it in the process
            Pair<Integer, DatagramPacket> pair = waitingPackets.poll();
            if (pair != null) {
                try {
                    socket.send(pair.second);
                } catch (IOException ignored) {
                }
                sendingPackets.put(pair.first, pair.second);
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                interrupt();
            }

            awakenSenders.run();

            if (isInterrupted()) {
                if (waitingPackets.isEmpty() && sendingPackets.isEmpty()) {
                    break;
                }
            }

            for (DatagramPacket packet : sendingPackets.values()) {
                try {
                    socket.send(packet);
                } catch (IOException ignored) {
                }
            }
        }
    }
}
