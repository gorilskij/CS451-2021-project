package cs451.perfect_links;

import cs451.ExecutorSingleton;
import cs451.base.BigEndianCoder;
import cs451.base.FullAddress;
import cs451.base.Pair;
import cs451.interfaces.PerfectLink;
import cs451.message.PLMessage;

import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

class PLMessageUUID extends Pair<Integer, Integer> {
    public PLMessageUUID(Integer plMessageId, Integer plSourceId) {
        super(plMessageId, plSourceId);
    }

    public int getPlMessageId() {
        return first;
    }

    public int getPlSourceId() {
        return second;
    }
}

// tracks delivered messages from a single source
class SourceDeliveredTracker {
    // all ids below this are known to be delivered
    private int deliveredBelow = 0;
    // all the ids in the queue are also delivered
    private final Queue<Integer> delivered = new PriorityBlockingQueue<>();

    synchronized boolean add(int messageId) {
        if (messageId < deliveredBelow) {
            return false;
        } else if (messageId == deliveredBelow) {
            deliveredBelow++;
            while (!delivered.isEmpty() && delivered.peek() == deliveredBelow) {
                delivered.poll();
                deliveredBelow++;
            }
        } else {
            delivered.add(messageId);
        }
        return true;
    }

    boolean contains(int messageId) {
        return messageId < deliveredBelow || delivered.contains(messageId);
    }
}

class DeliveredSet {
    private final Map<Integer, SourceDeliveredTracker> trackers = new ConcurrentHashMap<>();

    boolean add(PLMessageUUID messageUUID) {
        return trackers
                .computeIfAbsent(messageUUID.getPlSourceId(), ignored -> new SourceDeliveredTracker())
                .add(messageUUID.getPlMessageId());
    }

    boolean contains(PLMessageUUID messageUUID) {
        if (trackers.containsKey(messageUUID.getPlSourceId())) {
            return trackers.get(messageUUID.getPlSourceId()).contains(messageUUID.getPlMessageId());
        } else {
            return false;
        }
    }
}

public class PerfectLinkImpl implements PerfectLink {
    private final int processId;

    // *** sending ***
    private final Map<Integer, SendQueue> sendQueues = new ConcurrentHashMap<>();
    private final SendThread sendThread;

    // the id that can be anything is encoded in the payload, this id is known to start at 0 and increase with every message
    private final AtomicInteger nextMessageId = new AtomicInteger(0);

    // *** receiving ***
    private final ReceiveThread receiveThread;
    private final Reconstructor reconstructor;

    private final DeliveredSet delivered = new DeliveredSet();

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

                    PLMessageUUID key = new PLMessageUUID(packetId, sourceId);
                    if (delivered.add(key)) {
//                        ExecutorSingleton.submit(() -> reconstructor.add(packetData, normalPacket.getLength()));
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
        ExecutorSingleton.submit(() -> {
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
