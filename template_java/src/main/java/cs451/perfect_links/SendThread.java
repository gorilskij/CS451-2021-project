package cs451.perfect_links;

import cs451.Constants;
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

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(Constants.PL_NUM_SENDER_THREADS);
    private ScheduledFuture<?> taskHandle = null;

    private final Runnable flushSendQueues;

    public SendThread(DatagramSocket socket, Runnable flushSendQueues) {
        this.socket = socket;
        this.flushSendQueues = flushSendQueues;
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
                packet.bytes,
                packet.bytes.length,
                destination.address,
                destination.port
        );
    }

    public void sendPacket(Packet packet, FullAddress destination) {
        DatagramPacket udpPacket = makeUdpPacket(packet, destination);

        if (packet.packetId != 0) {
            // for non-ack packets
            sendingPackets.put(packet.packetId, udpPacket);
        }

        try {
            socket.send(udpPacket);
        } catch (IOException ignore) {
        }
    }

    public void acknowledge(int packetId) {
        sendingPackets.remove(packetId);
    }

    private final AtomicInteger batch = new AtomicInteger(0);
    public void run() {
        // TODO: decouple this
        flushSendQueues.run();

        if (sendingPackets.isEmpty()) {
            return;
        }

        int currentBatch = batch.getAndIncrement();
        int batchStart = currentBatch * Constants.PL_SENDING_BATCH_SIZE;
        if (batchStart >= sendingPackets.size()) {
            batch.set(0);
            batchStart = 0;
        }
        int batchEnd = batchStart + Constants.PL_SENDING_BATCH_SIZE;

        sendingPackets
                .values()
                .stream()
                .skip(batchStart)
                .limit(batchEnd)
                .forEach(packet -> {
                    try {
                        socket.send(packet);
                    } catch (IOException ignored) {
                    }
                });
    }
}
