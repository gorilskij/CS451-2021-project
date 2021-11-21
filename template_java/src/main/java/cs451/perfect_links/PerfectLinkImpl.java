package cs451.perfect_links;

import cs451.base.BigEndianCoder;
import cs451.base.FullAddress;
import cs451.base.Pair;
import cs451.interfaces.PerfectLink;
import cs451.message.PLMessage;

import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class PerfectLinkImpl implements PerfectLink {
    private final int processId;

    // *** sending ***
    private final Map<Integer, SendQueue> sendQueues = new ConcurrentHashMap<>();
    private final SendThread sendThread;
    private final AtomicInteger nextMessageId = new AtomicInteger(1);

    // *** receiving ***
    private final ReceiveThread receiveThread;
    private final Reconstructor reconstructor;
    // contains (packetId, sourceId)
    private final HashSet<Pair<Integer, Integer>> receivedIds = new HashSet<>(); // TODO: garbage collect

    private final ExecutorService executor = Executors.newFixedThreadPool(3);

    public PerfectLinkImpl(int processId, Map<Integer, FullAddress> addresses, DatagramSocket socket, Consumer<PLMessage> deliver) {
        this.processId = processId;
        this.reconstructor = new Reconstructor(deliver);

        sendThread = new SendThread(addresses, socket, () -> {
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

    private SendQueue getSendQueueFor(Integer destinationId) {
        return sendQueues.computeIfAbsent(destinationId, ignored -> new SendQueue(processId, destinationId, sendThread));
    }

    @Override
    public void plSend(String msg, Integer destinationId) {
        plSend(msg.getBytes(), destinationId);
    }

    @Override
    public void plSend(byte[] msgBytes, Integer destinationId) {
        executor.submit(() -> {
            int messageId = nextMessageId.getAndIncrement();
            getSendQueueFor(destinationId).send(messageId, msgBytes);
        });
    }

    @Override
    public void close() {
        sendThread.interrupt();
        receiveThread.interrupt();

        try {
            receiveThread.join();
        } catch (InterruptedException ignore) {
        }
    }
}
