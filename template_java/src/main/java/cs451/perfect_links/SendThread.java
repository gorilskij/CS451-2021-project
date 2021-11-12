package cs451.perfect_links;

import cs451.base.Pair;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class SendThread extends Thread {
    private final int SENDING_BATCH_SIZE = 100;

    private final DatagramSocket socket;

    // contains (packetId, packet)
    private final Queue<Pair<Integer, DatagramPacket>> waitingPackets = new LinkedBlockingQueue<>();
    private final Map<Integer, DatagramPacket> sendingPackets = new ConcurrentHashMap<>(SENDING_BATCH_SIZE);
    // ids of packets that have been sent and successfully received
    private final Set<Integer> removed = new HashSet<>();

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

    // TODO: package acks together with regular messages, give them priority
    public void acknowledge(int packetId) {
        if (packetId == 0) {
            throw new IllegalStateException("tried to acknowledge packet 0");
        }

        // TODO get rid of purely safety-oriented duplicate functionality of remove,
        //  condition the rest of the method on the result of removing from sendingPackets,
        //  essentially assume that an ack will never arrive for a message that wasn't sent
        if (removed.add(packetId)) {
            DatagramPacket removedPacket = sendingPackets.remove(packetId);
            if (removedPacket == null) {
                throw new IllegalStateException(
                        "sender received an acknowledge command for a packet that has never existed: " + packetId
                );
            }

            // put a packet into the sending batch and send it in the process
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
                // TODO: make this variable (based on what?)
                //  in effect, the speed decreases towards the end of transmission
                //  why..?
                Thread.sleep(10);
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
