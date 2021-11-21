package cs451.uniform_reliable_broadcast;

import cs451.Constants;
import cs451.base.BigEndianCoder;
import cs451.base.FullAddress;
import cs451.base.Pair;
import cs451.best_effort_broadcast.BEBImpl;
import cs451.interfaces.BEB;
import cs451.interfaces.PerfectLink;
import cs451.interfaces.URB;
import cs451.message.PLMessage;
import cs451.message.URBMessage;
import cs451.perfect_links.PerfectLinkImpl;

import java.net.DatagramSocket;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

class URBMessageUUID extends Pair<Integer, Integer> {
    public URBMessageUUID(Integer urbMessageId, Integer urbSourceId) {
        super(urbMessageId, urbSourceId);
    }

    public int getMessageId() {
        return first;
    }

    public int getSourceId() {
        return second;
    }
}

class PendingMessage {
    final String msg;
    final Set<Integer> acks = Collections.newSetFromMap(new ConcurrentHashMap<>());

    PendingMessage(String msg) {
        this.msg = msg;
    }
}

// URB messages are structured as follows:
// - urbMessageId - 4 bytes
// - urbSourceId  - 4 bytes
// - text         - n bytes
// note that urbMessageId and urbSourceId are different and have
// distinct meaning from messageId and sourceId (which are used
// by PerfectLink)
public class URBImpl implements URB {
    private static final int HEADER_SIZE = 8; // bytes

    private final int processId;
    private final int totalNumProcesses;

    private final BEB beb;

    private final Consumer<URBMessage> deliver;

    private int nextMessageId = 0;
    private final Set<URBMessageUUID> delivered = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Map<URBMessageUUID, PendingMessage> pending = new ConcurrentHashMap<>();

    // restrict the number of own messages than can be sent at a time
    private final AtomicInteger currentBatchSize = new AtomicInteger(0);
    private final BlockingQueue<String> awaitingBroadcast = new LinkedBlockingQueue<>();

    private final BlockingQueue<PLMessage> bebDeliverQueue = new LinkedBlockingQueue<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(5);
    private final Future<Object> executorHandle;

    public URBImpl(int processId, Map<Integer, FullAddress> addresses, DatagramSocket socket, Consumer<URBMessage> deliver) {
        this.processId = processId;
        totalNumProcesses = addresses.size();
        beb = new BEBImpl(processId, addresses, socket, bebMessage -> {
            try {
                if (bebMessage.getPlSourceId() == processId) {
                    System.out.println("process got a message from itself");
                    System.exit(1);
                }
                bebDeliverQueue.put(bebMessage); // blocking
            } catch (InterruptedException ignored) {
            }
        });
        this.deliver = deliver;

        executorHandle = executor.submit(() -> {
            while (true) {
                bebProcess();
            }
        });
    }

    private void bebProcess() {
        PLMessage message;
        try {
            message = bebDeliverQueue.take(); // blocking
        } catch (InterruptedException ignored) {
            return;
        }

        byte[] bytes = message.getTextBytes();
        int urbMessageId = BigEndianCoder.decodeInt(bytes, 0);
        int urbSourceId = BigEndianCoder.decodeInt(bytes, 4);
        URBMessageUUID messageUUID = new URBMessageUUID(urbMessageId, urbSourceId);

        if (delivered.contains(messageUUID)) {
            // if it was delivered, it was already (re)broadcast
            return;
        }

        Set<Integer> acks;
        if (pending.containsKey(messageUUID)) {
            // message already seen, waiting for 50% + 1
            acks = pending.get(messageUUID).acks;
            acks.add(message.getPlSourceId()); // original broadcaster and this process already acknowledged
        } else {
            // message never seen
            String text = new String(bytes, HEADER_SIZE, bytes.length - HEADER_SIZE);
            PendingMessage pendingMessage = new PendingMessage(text);
            pendingMessage.acks.add(urbSourceId); // original broadcaster
            pendingMessage.acks.add(message.getPlSourceId()); // last rebroadcaster
            pendingMessage.acks.add(processId); // this process
            pending.put(messageUUID, pendingMessage);
            acks = pendingMessage.acks;
        }
        tryDeliver(messageUUID);

        beb.bebBroadcast(message.getTextBytes());
    }

    private byte[] makeBytes(int messageId, String text) {
        byte[] textBytes = text.getBytes();
        byte[] bytes = new byte[HEADER_SIZE + textBytes.length];
        BigEndianCoder.encodeInt(messageId, bytes, 0);
        BigEndianCoder.encodeInt(processId, bytes, 4);
        System.arraycopy(textBytes, 0, bytes, HEADER_SIZE, textBytes.length);
        return bytes;
    }

    private void tryDeliver(URBMessageUUID messageUUID) {
        PendingMessage pendingMessage = pending.get(messageUUID);
        if (pendingMessage.acks.size() > totalNumProcesses / 2) {
            if (messageUUID.getSourceId() == processId) {
                // own message
                String nextMsg = awaitingBroadcast.poll();
                if (nextMsg != null) {
                    definitelyBroadcast(nextMsg);
                } else {
                    currentBatchSize.decrementAndGet();
                }
            }
            deliver.accept(new URBMessage(messageUUID.getMessageId(), messageUUID.getSourceId(), pendingMessage.msg));
            pending.remove(messageUUID);
            delivered.add(messageUUID);
        }
    }

    private void definitelyBroadcast(String msg) {
        int messageId = nextMessageId++;
        byte[] bytes = makeBytes(messageId, msg);
        PendingMessage pendingMessage = new PendingMessage(msg);
        pendingMessage.acks.add(processId);
        URBMessageUUID messageUUID = new URBMessageUUID(messageId, processId);
        pending.put(messageUUID, pendingMessage);
        tryDeliver(messageUUID);
        beb.bebBroadcast(bytes);
    }

    @Override
    public void urbBroadcast(String msg) {
        if (currentBatchSize.get() >= Constants.URB_SENDING_BATCH_SIZE) {
            try {
                awaitingBroadcast.put(msg);
            } catch (InterruptedException ignored) {
            }
        } else {
            currentBatchSize.incrementAndGet();
            definitelyBroadcast(msg);
        }
    }

    @Override
    public void close() {
        executorHandle.cancel(true);
        beb.close();
    }
}
