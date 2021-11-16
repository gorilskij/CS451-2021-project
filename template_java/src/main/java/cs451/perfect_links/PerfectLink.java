package cs451.perfect_links;

import cs451.base.BigEndianCoder;
import cs451.base.FullAddress;
import cs451.base.Pair;
import cs451.message.PLMessage;

import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class PerfectLink {
    private final int processId;
    private final Map<Integer, FullAddress> addresses;

    // *** sending ***
    private final Map<Integer, SendQueue> sendQueues = new ConcurrentHashMap<>();
    private final SendThread sendThread;
    private int nextMessageId = 1;

    // *** receiving ***
    private final ReceiveThread receiveThread;
    private final Reconstructor reconstructor;
    // contains (packetId, sourceId)
    private final HashSet<Pair<Integer, Integer>> receivedIds = new HashSet<>(); // TODO: garbage collect

    public PerfectLink(int processId, Map<Integer, FullAddress> addresses, DatagramSocket socket, Consumer<PLMessage> deliverCallback) {
        this.processId = processId;
        this.addresses = addresses;
        this.reconstructor = new Reconstructor(deliverCallback);

        sendThread = new SendThread(socket, () -> {
            for (SendQueue queue : sendQueues.values()) {
                queue.flush();
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
                    getSendQueueFor(sourceId).sendAck(packetId);
                },
                sendThread::acknowledge
        );

        sendThread.start();
        receiveThread.start();
    }

    private SendQueue getSendQueueFor(Integer pid) {
        FullAddress destination = addresses.get(pid);
        if (destination == null) {
            throw new IllegalStateException("no destination for pid " + pid);
        }
        return sendQueues.computeIfAbsent(pid, ignored -> new SendQueue(destination, processId, sendThread));
    }

    public void send(String msg, Integer processId) {
        int messageId = nextMessageId++;
        getSendQueueFor(processId).send(messageId, msg);
    }

    public void send(byte[] msgBytes, Integer processId) {
        int messageId = nextMessageId++;
        getSendQueueFor(processId).send(messageId, msgBytes);
    }

    public void close() {
        sendThread.interrupt();
        receiveThread.interrupt();

        try {
            receiveThread.join();
        } catch (InterruptedException ignore) {
        }
    }
}
