package cs451.perfect_links;

import cs451.base.BigEndianCoder;
import cs451.base.FullAddress;
import cs451.base.Pair;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class PerfectLink {
    private final int processId;

    // *** sending ***
    private final ConcurrentHashMap<FullAddress, SendQueue> sendQueues = new ConcurrentHashMap<>();
    private final SendThread sendThread;
    private int nextMessageId = 0;

    // *** receiving ***
    private final ReceiveThread receiveThread;
    private final Reconstructor reconstructor;
    // contains (packetId, sourceId)
    private final HashSet<Pair<Integer, Integer>> receivedIds = new HashSet<>(); // TODO: garbage collect

    public PerfectLink(int processId, DatagramSocket socket, Consumer<PLMessage> deliverCallback) {
        this.processId = processId;
        this.reconstructor = new Reconstructor(deliverCallback);

        sendThread = new SendThread(socket, () -> {
            for (SendQueue queue : sendQueues.values()) {
                queue.awaken();
            }
        });

        receiveThread = new ReceiveThread(
                socket,
                normalPacket -> {
                    byte[] packetData = normalPacket.getData();
                    int packetId = BigEndianCoder.decodeInt(packetData, 0);
                    int sourceId = BigEndianCoder.decodeInt(packetData, 4);

                    Pair<Integer, Integer> key = new Pair<>(packetId, sourceId);
                    if (receivedIds.add(key)) {
                        reconstructor.add(packetData, normalPacket.getLength());
                    }

                    // send acknowledgement
                    try {
                        Packet acknowledgement = new Packet(packetId, packetData).acknowledgement(sourceId);
                        socket.send(new DatagramPacket(
                                acknowledgement.data,
                                acknowledgement.data.length,
                                normalPacket.getAddress(),
                                normalPacket.getPort()
                        ));
                    } catch (IOException ignored) {
                    }
                },
                sendThread::acknowledge
        );

        sendThread.start();
        receiveThread.start();
    }

    private SendQueue getSendQueueFor(FullAddress destination) {
        return sendQueues.computeIfAbsent(destination, ignored -> new SendQueue(destination, processId, sendThread));
    }

    public void send(String msg, FullAddress destination) {
        int messageId = nextMessageId++;
        getSendQueueFor(destination).send(messageId, msg);
    }

    public void send(byte[] msgBytes, FullAddress destination) {
        int messageId = nextMessageId++;
        getSendQueueFor(destination).send(messageId, msgBytes);
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
