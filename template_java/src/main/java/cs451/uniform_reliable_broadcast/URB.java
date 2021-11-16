package cs451.uniform_reliable_broadcast;

import cs451.Constants;
import cs451.base.BigEndianCoder;
import cs451.base.FullAddress;
import cs451.base.Pair;
import cs451.message.PLMessage;
import cs451.message.URBMessage;
import cs451.perfect_links.PerfectLink;

import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

class AckCounter {
    final URBMessage message;
    Set<Integer> acknowledged = new HashSet<>();

    AckCounter(URBMessage message) {
        this.message = message;
    }
}

// URB messages are structured as follows:
// - urbMessageId - 4 bytes
// - urbSourceId  - 4 bytes
// - text         - n bytes
// note that urbMessageId and urbSourceId are different and have
// distinct meaning from messageId and sourceId (which are used
// by PerfectLink)
public class URB {
    private static final int HEADER_SIZE = 8; // bytes

    private final int processId;
    private final Map<Integer, FullAddress> addresses;
    private final int totalNumProcesses;
//    private final FullAddress[] otherAddresses;
    private final PerfectLink perfectLink;
    // TODO: benchmark whether waiting is really needed
    private final Queue<String> waiting = new LinkedBlockingQueue<>();
    // (urbMessageId, urbSourceId): processes that acknowledge
    private final Map<Pair<Integer, Integer>, AckCounter> received = new ConcurrentHashMap<>();

    // (urbMessageId, urbSourceId)
    private final Set<Pair<Integer, Integer>> delivered = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private int nextUrbMessageId = 0;

    private final Consumer<URBMessage> deliverCallback;

    // allProcesses includes this process
    public URB(int processId, Map<Integer, FullAddress> addresses, DatagramSocket socket, Consumer<URBMessage> deliverCallback) {
        this.processId = processId;
        this.addresses = addresses;
        totalNumProcesses = addresses.size();
        perfectLink = new PerfectLink(processId, addresses, socket, this::onPlDeliver);
        this.deliverCallback = deliverCallback;
    }

    private synchronized void onPlDeliver(PLMessage message) {
        byte[] bytes = message.getTextBytes();

        // in this case sourceId refers to the process that sent
        // this particular message while urbSourceId refers to the
        // process that originally broadcast the message
        int urbMessageId = BigEndianCoder.decodeInt(bytes, 0);
        int urbSourceId = BigEndianCoder.decodeInt(bytes, 4);

        Pair<Integer, Integer> key = new Pair<>(urbMessageId, urbSourceId);
        if (!delivered.contains(key)) {
            boolean[] rebroadcast = {false};

//            System.out.println("URB RECEIVE " + urbMessageId + " FROM " + urbSourceId);
            AckCounter ackCounter = received.computeIfAbsent(key, ignored -> {
//                System.out.println("> is new");
                rebroadcast[0] = true;
                String text = new String(bytes, HEADER_SIZE, bytes.length - HEADER_SIZE);
                AckCounter counter = new AckCounter(new URBMessage(urbMessageId, urbSourceId, text));
                counter.acknowledged.add(processId);
                return counter;
            });

            ackCounter.acknowledged.add(message.sourceId);
            if (ackCounter.acknowledged.size() > totalNumProcesses / 2) {
//                System.out.println("> URB DELIVER");
//                    System.out.println("50% + 1 acknowledged, delivering");
                received.remove(key);

                // broadcast next message in queue
                String msg = waiting.poll();
                if (msg != null) {
                    broadcast(msg);
                }

                delivered.add(key);
                System.out.println("URB DELIVER " + urbMessageId + " from " + urbSourceId);
                deliverCallback.accept(ackCounter.message);
            }

            // TODO: rebroadcast to everyone except the original sender and the current sender
            //  for those two, just send an ack
            // rebroadcast
            if (rebroadcast[0]) {
                broadcastSend(message.getTextBytes());
            }
        }
    }

    private void broadcastSend(byte[] bytes) {
        int urbMessageId = BigEndianCoder.decodeInt(bytes, 0);
        int urbSourceId = BigEndianCoder.decodeInt(bytes, 4);
        System.out.println("URB SEND " + urbMessageId + " FROM " + urbSourceId);

        for (int pid : addresses.keySet()) {
            if (pid != processId) {
                perfectLink.send(bytes, pid);
            }
        }
    }

    public synchronized void broadcast(String msg) {
//        System.out.println("try broadcast message: " + msg);
        if (received.size() >= Constants.URB_SENDING_BATCH_SIZE) {
//            System.out.println("enqueue");
            waiting.offer(msg);
        } else {
//            System.out.println("broadcast");
            int urbMessageId = nextUrbMessageId++;
            byte[] msgBytes = msg.getBytes();
            byte[] bytes = new byte[HEADER_SIZE + msgBytes.length];
            BigEndianCoder.encodeInt(urbMessageId, bytes, 0);
            BigEndianCoder.encodeInt(processId, bytes, 4);
            System.arraycopy(msgBytes, 0, bytes, HEADER_SIZE, msgBytes.length);

            AckCounter ackCounter = new AckCounter(new URBMessage(urbMessageId, processId, msg));
            ackCounter.acknowledged.add(processId);
            received.put(new Pair<>(urbMessageId, processId), ackCounter);

            broadcastSend(bytes);
        }
//        System.out.println("new state: received: " + received.size() + ", waiting: " + waiting.size());
    }

    public void close() {
        perfectLink.close();
    }
}
