package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

// just a tuple class to uniquely identify each message (message id, sender id)
class MessageKey {
    public final int messageId;
    public final int sourceId;

    MessageKey(int messageId, int sourceId) {
        this.messageId = messageId;
        this.sourceId = sourceId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageKey that = (MessageKey) o;
        return messageId == that.messageId && sourceId == that.sourceId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, sourceId);
    }
}

class SendThread extends Thread {
    private final int SENDING_BATCH_SIZE = 100;

    private final DatagramSocket socket;
    private final HashMap<Integer, Message> allMessages = new HashMap<>();
    private final HashMap<Integer, Message> sendingBatch = new HashMap<>();
    // ids of messages that have been sent and successfully received
    private final HashSet<Integer> removed = new HashSet<>();

    public SendThread(DatagramSocket socket) {
        this.socket = socket;
    }

    public void send(Message message) {
        synchronized (sendingBatch) {
            if (sendingBatch.size() < SENDING_BATCH_SIZE) {
                message.send(socket);
                sendingBatch.put(message.messageId, message);
            } else {
                synchronized (allMessages) {
                    allMessages.put(message.messageId, message);
                }
            }
        }
    }

    public void remove(int messageId) {
        System.out.println("call remove");
        synchronized (sendingBatch) {
            if (removed.add(messageId)) {
                Message removedMessage = sendingBatch.remove(messageId);
                if (removedMessage == null) {
                    throw new IllegalStateException(
                            "sender received a remove command for a message that has never existed: " + messageId
                    );
                }

                // add a message from allMessages to sendingBatch and send it in the process
                synchronized (allMessages) {
                    Optional<Integer> firstKey = allMessages.keySet().stream().findFirst();
                    if (firstKey.isPresent()) {
                        Message newMessage = allMessages.remove(firstKey.get());
                        newMessage.send(socket);
                        sendingBatch.put(newMessage.messageId, newMessage);
                        System.out.println("sent message " + newMessage.messageId + " due to ack received for " + messageId);
                    }
                }
            }
        }
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                interrupt();
            }

            if (isInterrupted()) {
                synchronized (allMessages) {
                    synchronized (sendingBatch) {
                        if (allMessages.isEmpty() && sendingBatch.isEmpty()) {
                            break;
                        }
                    }
                }
            }

            System.out.println("henlou");
            synchronized (sendingBatch) {
                for (Message message : sendingBatch.values()) {
                    // TODO: maybe send in bursts? like 5 copies at a time?
                    // TODO: consider batching messages together
                    message.send(socket);
                    System.out.println("!! sent repeat");
                }
            }
        }
    }
}

class ReceiveThread extends Thread {
    private final DatagramSocket socket;
    private final byte[] buffer = new byte[256];

    private final Consumer<Message> normalMessageCallback;
    private final Consumer<Message> acknowledgementCallback;

    ReceiveThread(DatagramSocket socket, Consumer<Message> normalMessageCallback, Consumer<Message> acknowledgementCallback) {
        this.socket = socket;
        this.normalMessageCallback = normalMessageCallback;
        this.acknowledgementCallback = acknowledgementCallback;
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            try {
                socket.receive(packet);
            } catch (SocketTimeoutException e) {
                continue;
            } catch (IOException e) {
                throw new Error(e);
            }

            Message received = Message.received(packet);

            if (received.messageType == Message.NORMAL_MESSAGE) {
                normalMessageCallback.accept(received);
            } else if (received.messageType == Message.ACKNOWLEDGEMENT) {
                acknowledgementCallback.accept(received);
            } else {
                throw new IllegalStateException("Unknown message type: " + received.messageType);
            }
        }
    }
}

public class PerfectLink {
    private final int processId;

    // *** sending ***
    private final SendThread sendThread;
    private int nextMessageId = 0;

    // *** receiving ***
    private final ReceiveThread receiveThread;
    // TODO: replace with actual queue or stack
    private final ArrayList<Message> receivedQueue = new ArrayList<>();
    private final HashSet<MessageKey> receivedIds = new HashSet<>();

    public PerfectLink(int processId, DatagramSocket socket) {
        this.processId = processId;

        sendThread = new SendThread(socket);

        receiveThread = new ReceiveThread(
                socket,
                normalMessage -> {
                    MessageKey key = new MessageKey(normalMessage.messageId, normalMessage.sourceId);
                    if (receivedIds.contains(key)) {
                        normalMessage.sendAcknowledgement(processId, socket);
                    } else {
                        receivedIds.add(key);
                        synchronized (receivedQueue) {
                            receivedQueue.add(normalMessage);
                        }
                    }
                },
                acknowledgement -> {
                    sendThread.remove(acknowledgement.messageId);
                }
        );

        sendThread.start();
        receiveThread.start();
    }

    public void send(String msg, FullAddress destination) {
        System.out.println("Enqueue \"" + msg + "\" to " + destination);

        int messageId = nextMessageId++;
        Message message = Message.normalMessage(messageId, processId, msg, destination);
        sendThread.send(message);
    }

    // non-blocking, returns null if there is nothing to deliver at the moment
    public Message deliver() {
        synchronized (receivedQueue) {
            if (receivedQueue.size() == 0) {
                return null;
            } else {
                return receivedQueue.remove(0);
            }
        }
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
