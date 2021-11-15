package cs451.perfect_links;

import cs451.Constants;
import cs451.base.Pair;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class SendThread extends Thread {
    private final DatagramSocket socket;

    private final Map<Integer, DatagramPacket> sendingPackets = new LinkedHashMap<>();

    // ids of packets that have been sent and successfully received
    private final Set<Integer> removed = new HashSet<>();

    private final Runnable awakenSenders;

    public SendThread(DatagramSocket socket, Runnable awakenSenders) {
        this.socket = socket;
        this.awakenSenders = awakenSenders;
    }

    public void send(int packetId, DatagramPacket packet) {
        try {
            socket.send(packet);
        } catch (IOException ignore) {
        }
        synchronized (sendingPackets) {
            sendingPackets.put(packetId, packet);
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
            DatagramPacket removedPacket;
            synchronized (sendingPackets) {
                removedPacket = sendingPackets.remove(packetId);
            }
            if (removedPacket == null) {
                throw new IllegalStateException(
                        "sender received an acknowledge command for a packet that has never existed: " + packetId
                );
            }
        }
    }

    @Override
    public void run() {
        ArrayList<DatagramPacket> send = new ArrayList<>();
        for (int iteration = 0;; iteration++) {
            try {
                // TODO: make this variable (based on what?)
                //  in effect, the speed decreases towards the end of transmission
                //  why..?
                Thread.sleep(10);
            } catch (InterruptedException e) {
                interrupt();
            }

            awakenSenders.run();

            synchronized (sendingPackets) {
                if (sendingPackets.isEmpty()) {
                    if (isInterrupted()) {
                        break;
                    } else {
                        continue;
                    }
                }

                int limit;
                // TODO: spread it out
                if (iteration % 10 == 0) {
                    // every 100ms, resend all
                    limit = -1;
                } else {
                    limit = Constants.PL_SENDING_BATCH_SIZE;
                }
                send.clear();
                int i = 0;
                for (DatagramPacket packet : sendingPackets.values()) {
                    if (limit > -1) {
                        if (i >= limit) {
                            break;
                        }
                        i++;
                    }

                    send.add(packet);
                }
            }

            for (DatagramPacket packet : send) {
                try {
                    socket.send(packet);
                } catch (IOException ignored) {
                }
            }
        }
    }
}
