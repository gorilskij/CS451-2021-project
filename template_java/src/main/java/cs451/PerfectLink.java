package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
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

class SendingMessage {
    static final long TIMEOUT_NANOS = 100_000_000L; // 100ms

    private final Message message;
    long lastSentNanoTime = 0;

    SendingMessage(Message message) {
        this.message = message;
    }

    void send(long nanoTime, DatagramSocket socket) {
        message.send(socket);
        lastSentNanoTime = nanoTime;
    }

    long leftToWait(long nanoTime) {
        return TIMEOUT_NANOS - (nanoTime - lastSentNanoTime);
    }
}


class SendThread extends Thread {
    private final DatagramSocket socket;
    private final HashMap<Integer, SendingMessage> sending;

    public SendThread(DatagramSocket socket, HashMap<Integer, SendingMessage> sending) {
        this.socket = socket;
        this.sending = sending;
    }

    @Override
    public void run() {
        long nextSendAttemptNanoTime = 0;

        while (true) {
            synchronized (sending) {
                if (sending.isEmpty()) {
                    if (isInterrupted()) {
                        System.out.println("PerfectLinkSender says GOODBYE");
                        break;
                    } else {
                        try {
                            Thread.sleep(SendingMessage.TIMEOUT_NANOS / 1_000_000);
                        } catch (InterruptedException e) {
                            // set isInterrupted() to return true (this is not automatic!)
                            interrupt();
                        }
                    }
                }

                long nanoTime = System.nanoTime();
                if (nanoTime >= nextSendAttemptNanoTime) {
                    System.out.println("henlou");
                    int msgsSent = 0;
                    long minLeftToWait = SendingMessage.TIMEOUT_NANOS;
                    for (SendingMessage message : sending.values()) {
                        long leftToWait = message.leftToWait(nanoTime);
                        if (leftToWait <= 0) {
                            message.send(nanoTime, socket);
                            msgsSent += 1;
                        } else if (leftToWait < minLeftToWait) {
                            minLeftToWait = leftToWait;
                        }
                    }
                    System.out.println("sent msgs: " + msgsSent);
                    nextSendAttemptNanoTime = nanoTime + minLeftToWait;
                }
            }

            long sleepNanoTime = nextSendAttemptNanoTime - System.nanoTime();
            if (sleepNanoTime < 1_000_000 /* 1ms */) {
                sleepNanoTime = 1_000_000;
            } else if (sleepNanoTime > 10_000_000 /* 10ms */) {
                sleepNanoTime = 10_000_000;
            }
            try {
                Thread.sleep(sleepNanoTime / 1_000_000);
            } catch (InterruptedException e) {
                // set isInterrupted() to return true (this is not automatic!)
                interrupt();
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
    private final DatagramSocket socket;

    // *** sending ***
    private final SendThread sendThread;
    private int nextMessageId = 0;
    // messages in the process of being sent
    private final HashMap<Integer, SendingMessage> sending = new HashMap<>();
    // messages that have been sent and successfully received
    private final HashSet<Integer> removed = new HashSet<>();

    // *** receiving ***
    private final ReceiveThread receiveThread;
    private final ArrayList<Message> receivedQueue = new ArrayList<>();
    private final HashSet<MessageKey> receivedIds = new HashSet<>();

    public PerfectLink(int processId, DatagramSocket socket) {
        this.processId = processId;
        this.socket = socket;

        sendThread = new SendThread(socket, sending);

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
                    if (!removed.contains(acknowledgement.messageId)) {
                        synchronized (sending) {
                            if (sending.remove(acknowledgement.messageId) != null) {
                                removed.add(acknowledgement.messageId);
                            } else {
                                throw new IllegalStateException(
                                        "sender for process "
                                                + processId
                                                + " received a remove command for a message that has never existed: "
                                                + acknowledgement.messageId
                                );
                            }
                        }
                    }
                }
        );

        sendThread.start();
        receiveThread.start();
    }

    public void send(String msg, FullAddress destination) {
        System.out.println("Enqueue \"" + msg + "\" to " + destination);

        int messageId = nextMessageId++;
        Message message = Message.normalMessage(messageId, processId, msg, destination);
        message.send(socket);
        SendingMessage sendingMessage = new SendingMessage(message);
        synchronized (sending) {
            sending.put(messageId, sendingMessage);
        }
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
