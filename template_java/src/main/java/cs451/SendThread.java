package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

public class SendThread extends Thread {
    private final int SENDING_BATCH_SIZE = 100;

    private final DatagramSocket socket;

    private final HashMap<Integer, DatagramPacket> waitingPackets = new HashMap<>();
    private final HashMap<Integer, DatagramPacket> sendingPackets = new HashMap<>();
    // ids of packets that have been sent and successfully received
    private final HashSet<Integer> removed = new HashSet<>();

    private final Runnable awakenSenders;

    public SendThread(DatagramSocket socket, Runnable awakenSenders) {
        this.socket = socket;
        this.awakenSenders = awakenSenders;
    }

    public void send(int packetId, DatagramPacket packet) {
        System.out.println("ST.send");
        synchronized (sendingPackets) {
            if (sendingPackets.size() < SENDING_BATCH_SIZE) {
                try {
                    socket.send(packet);
                } catch (IOException ignore) {
                }
                sendingPackets.put(packetId, packet);
                System.out.println("> sendingPackets");
            } else {
                synchronized (waitingPackets) {
                    waitingPackets.put(packetId, packet);
                }
                System.out.println("> waitingPackets");
            }
        }
    }

    public void remove(int packetId) {
//        System.out.println("call remove for " + packetId);
        synchronized (sendingPackets) {
            if (removed.add(packetId)) {
//                System.out.println("SUCCESSFULLY REMOVE PACKET " + packetId);
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
//                            System.out.println("sent packet " + firstKey.get() + " due to ack received for " + packetId);
                        } catch (IOException ignored) {
                        }
                        sendingPackets.put(firstKey.get(), newPacket);
                    }
                }
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
                synchronized (waitingPackets) {
                    synchronized (sendingPackets) {
                        if (waitingPackets.isEmpty() && sendingPackets.isEmpty()) {
                            break;
                        }
                    }
                }
            }

//            System.out.println("henlou");
            synchronized (sendingPackets) {
//                for (DatagramPacket packet : sendingPackets.values()) {
                for (Map.Entry<Integer, DatagramPacket> entry : sendingPackets.entrySet()) {
//                    int packetId = entry.getKey();
                    DatagramPacket packet = entry.getValue();

                    try {
                        socket.send(packet);
//                        System.out.println("send packet " + packetId);
                    } catch (IOException ignored) {
                    }
//                    System.out.println("!! sent repeat");
                }
            }
        }
    }
}
