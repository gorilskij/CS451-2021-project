package cs451.perfect_links;

import cs451.Constants;
import cs451.base.BigEndianCoder;
import cs451.base.FullAddress;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.*;

public class SendThread {
    private final DatagramSocket socket;

    private final Map<Integer, DatagramPacket> sendingPackets = new ConcurrentHashMap<>();

    // ids of packets that have been sent and successfully received
    private final Set<Integer> removed = new HashSet<>();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(Constants.PL_NUM_SENDER_THREADS);
    private ScheduledFuture<?> taskHandle = null;

    private final Runnable awakenSenders;

    public SendThread(DatagramSocket socket, Runnable awakenSenders) {
        this.socket = socket;
        this.awakenSenders = awakenSenders;
    }

    public void start() {
        if (taskHandle != null) {
            throw new IllegalStateException("multiple start");
        }
        taskHandle = scheduler.scheduleAtFixedRate(this::run, 0, Constants.PL_SENDING_INTERVAL, MILLISECONDS);
    }

    public void interrupt() {
        if (taskHandle != null) {
            taskHandle.cancel(true);
        }
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
//        synchronized (sendingPackets) {
        sendingPackets.put(packet.packetId, udpPacket);
//        }
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
//            synchronized (sendingPackets) {
            removedPacket = sendingPackets.remove(packetId);
//            }
            if (removedPacket == null) {
                throw new IllegalStateException(
                        "sender received an acknowledge command for a packet that has never existed: " + packetId
                );
            }
        }
    }

    private final AtomicInteger batch = new AtomicInteger(0);
    private final ArrayList<DatagramPacket> send = new ArrayList<>();

    public void run() {
        // TODO: decouple this
        awakenSenders.run();

        if (sendingPackets.isEmpty()) {
            return;
        }

        int currentBatch = batch.getAndIncrement();
        int batchStart = currentBatch * Constants.PL_SENDING_BATCH_SIZE;
        if (batchStart > sendingPackets.size()) {
            batch.set(0);
            batchStart = 0;
        }
        int batchEnd = batchStart + Constants.PL_SENDING_BATCH_SIZE;

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

        for (DatagramPacket packet : send) {
            try {
                socket.send(packet);
            } catch (IOException ignored) {
            }
        }
    }
}
