package cs451.perfect_links;

import cs451.Constants;
import cs451.base.BigEndianCoder;
import cs451.base.FullAddress;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.*;

public class SendThread extends Thread {
    private final DatagramSocket socket;

    // TODO: convert back to ConcurrentHashMap
    private final Map<Integer, DatagramPacket> sendingPackets = new LinkedHashMap<>();

    // ids of packets that have been sent and successfully received
    private final Set<Integer> removed = new HashSet<>();

    private final Runnable awakenSenders;

    public SendThread(DatagramSocket socket, Runnable awakenSenders) {
        this.socket = socket;
        this.awakenSenders = awakenSenders;
    }

    private DatagramPacket makeUdpPacket(Packet packet, FullAddress destination) {
        return new DatagramPacket(
                packet.data,
                packet.data.length,
                destination.address,
                destination.port
        );
    }

    public void sendPacket(Packet packet, FullAddress destination) {
        if (packet.packetId == 0) {
            throw new IllegalStateException("tried to send ack packet using regular mechanism");
        }
        DatagramPacket udpPacket = makeUdpPacket(packet, destination);
        try {
            socket.send(udpPacket);
        } catch (IOException ignore) {
        }
        synchronized (sendingPackets) {
            sendingPackets.put(packet.packetId, udpPacket);
        }
    }

    public void sendAckPacket(Packet packet, FullAddress destination) {
        DatagramPacket udpPacket = makeUdpPacket(packet, destination);
        try {
            socket.send(udpPacket);
        } catch (IOException ignore) {
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
        int batch = 0;
        ArrayList<DatagramPacket> send = new ArrayList<>();
        for (int iteration = 0;; iteration++) {
            try {
                // TODO: make this variable (based on what?)
                //  in effect, the speed decreases towards the end of transmission
                //  why..?
                Thread.sleep(Constants.PL_SENDING_INTERVAL);
            } catch (InterruptedException e) {
                interrupt();
            }

            awakenSenders.run();

            synchronized (sendingPackets) {
//                System.out.println("qsize: " + sendingPackets.size());
                if (sendingPackets.size() == 1) {
                    for (DatagramPacket p : sendingPackets.values()) {
                        int id = BigEndianCoder.decodeInt(p.getData(), 0);
                        int s = BigEndianCoder.decodeInt(p.getData(), 4);
//                        System.out.println("PACKET " + id + " from " + s);
                    }
                }

                if (sendingPackets.isEmpty()) {
                    if (isInterrupted()) {
                        break;
                    } else {
                        continue;
                    }
                }

                batch++;
                int batchStart = batch * Constants.PL_SENDING_BATCH_SIZE;
                if (batchStart > sendingPackets.size()) {
                    batch = 0;
                    batchStart = 0;
                }
                int batchEnd = batchStart + Constants.PL_SENDING_BATCH_SIZE;
//                System.out.println("sending batch [" + batchStart + ", " + batchEnd + ")");

                send.clear();
                int i = 0;
                for (DatagramPacket packet : sendingPackets.values()) {
                    if (i >= batchStart) {
                        send.add(packet);
                        if (i >= batchEnd) {
                            break;
                        }
                    }
                    i++;
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
