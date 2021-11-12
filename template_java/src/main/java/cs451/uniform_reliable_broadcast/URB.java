package cs451.uniform_reliable_broadcast;

import cs451.base.BigEndianCoder;
import cs451.base.FullAddress;
import cs451.base.Pair;
import cs451.perfect_links.PerfectLink;

import java.net.DatagramSocket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

class ReceivedMessage {
    final String message;
    Set<Integer> acknowledged = new HashSet<>();

    ReceivedMessage(String message) {
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
    private static final int SEND_BATCH_SIZE = 1000;

    private final int processId;
    private final Map<Integer, FullAddress> addresses;
    private final int totalNumProcesses;
//    private final FullAddress[] otherAddresses;
    private final PerfectLink perfectLink;
    // TODO: benchmark whether waiting is really needed
    private final Queue<String> waiting = new LinkedBlockingQueue<>();
    // (urbMessageId, urbSourceId): processes that acknowledge
    private final Map<Pair<Integer, Integer>, ReceivedMessage> received = new ConcurrentHashMap<>();
    // (urbMessageId, urbSourceId)
    private final Set<Pair<Integer, Integer>> delivered = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private int nextMessageId = 1; // acknowledgements have id 0

    // allProcesses includes this process
    public URB(int processId, Map<Integer, FullAddress> addresses, DatagramSocket socket, Consumer<String> deliverCallback) {
        this.processId = processId;
        this.addresses = addresses;
        totalNumProcesses = addresses.size();
//        otherAddresses = new FullAddress[allProcesses.size() - 1];

//        int i = 0;
//        for (Pair<Integer, FullAddress> pair : allProcesses) {
//            Integer otherProcessId = pair.first;
//            if (otherProcessId == processId) {
//                continue;
//            }
//            otherAddresses[i++] = pair.second;
//        }

        perfectLink = new PerfectLink(processId, addresses, socket, message -> {
            byte[] bytes = message.getTextBytes();

            // in this case sourceId refers to the process that sent
            // this particular message while urbSourceId refers to the
            // process that originally broadcast the message
            int urbMessageId = BigEndianCoder.decodeInt(bytes, 0);
            int urbSourceId = BigEndianCoder.decodeInt(bytes, 4);

//            System.out.println("\n\n=========");
//            System.out.println("message PL-delivered from " + message.sourceId + " to " + processId);
//            String text1 = new String(bytes, HEADER_SIZE, bytes.length - HEADER_SIZE);
//            System.out.println("text: \"" + text1 + "\"");
//            System.out.println("urbMessageId: " + urbMessageId);
//            System.out.println("urbSourceId:  " + urbSourceId);
//            System.out.println("=========");

            Pair<Integer, Integer> key = new Pair<>(urbMessageId, urbSourceId);
            if (!delivered.contains(key)) {
                boolean[] rebroadcast = {false};

                ReceivedMessage receivedMessage = received.computeIfAbsent(key, ignored -> {
//                    System.out.println("received new message");
                    rebroadcast[0] = true;
                    String text = new String(bytes, HEADER_SIZE, bytes.length - HEADER_SIZE);
                    ReceivedMessage rm = new ReceivedMessage(text);
                    rm.acknowledged.add(processId);
                    return rm;
                });

                receivedMessage.acknowledged.add(message.sourceId);
                if (receivedMessage.acknowledged.size() >= totalNumProcesses / 2 + 1) {
//                    System.out.println("50% + 1 acknowledged, delivering");
                    received.remove(key);
                    // broadcast next message in queue
                    if (!waiting.isEmpty()) {
                        String msg = waiting.poll();
                        broadcast(msg);
                    }
                    delivered.add(key);
                    deliverCallback.accept(receivedMessage.message);
                }

                // TODO: rebroadcast to everyone except the original sender and the current sender
                //  for those two, just send an ack
                // rebroadcast
                if (rebroadcast[0]) {
//                    System.out.println("rebroadcast");
                    broadcastSend(message.getTextBytes());
                }
            }
        });
    }

    private void broadcastSend(byte[] bytes) {
        for (int pid : addresses.keySet()) {
            if (pid != processId) {
                perfectLink.send(bytes, pid);
            }
        }
    }

    public synchronized void broadcast(String msg) {
//        System.out.println("try broadcast message: " + msg);
        if (received.size() >= SEND_BATCH_SIZE) {
//            System.out.println("enqueue");
            waiting.offer(msg);
        } else {
//            System.out.println("broadcast");
            int messageId = nextMessageId++;
            byte[] msgBytes = msg.getBytes();
            byte[] bytes = new byte[HEADER_SIZE + msgBytes.length];
            BigEndianCoder.encodeInt(messageId, bytes, 0);
            BigEndianCoder.encodeInt(processId, bytes, 4);
            System.arraycopy(msgBytes, 0, bytes, HEADER_SIZE, msgBytes.length);

            ReceivedMessage receivedMessage = new ReceivedMessage(msg);
            receivedMessage.acknowledged.add(processId);
            received.put(new Pair<>(messageId, processId), receivedMessage);

            broadcastSend(bytes);
        }
//        System.out.println("new state: received: " + received.size() + ", waiting: " + waiting.size());
    }

    public void close() {
        perfectLink.close();
    }
}
