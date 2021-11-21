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
    private static final boolean DEBUG_PRINT = false;

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
    private int nextUrbMessageId = 1;

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

        if (DEBUG_PRINT) {
            System.out.println("PL DELIVER (" + urbMessageId + " urb-FROM " + urbSourceId + ") pl-FROM " + message.sourceId);
        }

        Pair<Integer, Integer> key = new Pair<>(urbMessageId, urbSourceId);
        if (!delivered.contains(key)) {
            if (DEBUG_PRINT) {
                System.out.println("> not yet delivered");
            }
            boolean[] rebroadcast = {false};

//            System.out.println("URB RECEIVE " + urbMessageId + " FROM " + urbSourceId);
            AckCounter ackCounter = received.computeIfAbsent(key, ignored -> {
                if (DEBUG_PRINT) {
                    System.out.println("> message is new, create ack counter");
                }
                rebroadcast[0] = true;
                String text = new String(bytes, HEADER_SIZE, bytes.length - HEADER_SIZE);
                AckCounter counter = new AckCounter(new URBMessage(urbMessageId, urbSourceId, text));
                counter.acknowledged.add(processId);
                return counter;
            });

            ackCounter.acknowledged.add(message.sourceId);
            if (ackCounter.acknowledged.size() > totalNumProcesses / 2) {
                if (DEBUG_PRINT) {
                    System.out.println("> ack counter over threshold (" + ackCounter.acknowledged.size() + "), deliver");
                }
                received.remove(key);
                delivered.add(key);
                deliverCallback.accept(ackCounter.message);
//                rebroadcast[0] = false;

                // broadcast next message in queue
                String msg = waiting.poll();
                if (msg != null) {
                    if (DEBUG_PRINT) {
                        System.out.println("> broadcast next message in queue \"" + msg + "\"");
                    }
                    broadcast(msg);
                }
            }

            // TODO: rebroadcast to everyone except the original sender and the current sender
            //  for those two, just send an ack
            // rebroadcast
            if (rebroadcast[0]) {
                if (DEBUG_PRINT) {
                    System.out.println("> rebroadcast");
                }
                broadcastSend(message.getTextBytes());
            }
        } else {
            if (DEBUG_PRINT) {
                System.out.println("> already urb-delivered, ignore");
            }
        }
    }

    private void broadcastSend(byte[] bytes) {
        int urbMessageId = BigEndianCoder.decodeInt(bytes, 0);
        int urbSourceId = BigEndianCoder.decodeInt(bytes, 4);
        if (DEBUG_PRINT) {
            System.out.println("URB SEND " + urbMessageId + " FROM " + urbSourceId);
        }

        for (int pid : addresses.keySet()) {
            if (pid != processId) {
                perfectLink.send(bytes, pid);
            }
        }
    }

    public synchronized void broadcast(String msg) {
        if (DEBUG_PRINT) {
            System.out.println("URB FIRST BROADCAST \"" + msg + "\"");
        }

//        System.out.println("try broadcast message: " + msg);
        if (received.size() >= Constants.URB_SENDING_BATCH_SIZE) {
            if (DEBUG_PRINT) {
                System.out.println("> batch full, enqueue");
            }
            waiting.offer(msg);
        } else {
            if (DEBUG_PRINT) {
                System.out.println("> batch has space, send");
            }
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
    }

    public void close() {
        perfectLink.close();
    }
}
