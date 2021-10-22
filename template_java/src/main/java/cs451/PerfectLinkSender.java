package cs451;

import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.HashSet;

class SendingMessage {
    static final long TIMEOUT = 1_000_000_000L; // 1s

    final Message message;
    long lastSentNanoTime = 0;

    SendingMessage(Message message) {
        this.message = message;
    }

    /**
     * @param nanoTime
     * @param socket
     * @return number of nanoseconds left to wait before this message
     * can be sent again
     */
    long trySend(long nanoTime, DatagramSocket socket) {
        if (nanoTime - lastSentNanoTime >= TIMEOUT) {
            message.send(socket);
            lastSentNanoTime = nanoTime;
        }

        // can't be negative
        return TIMEOUT - (nanoTime - lastSentNanoTime);
    }
}

public class PerfectLinkSender extends Thread {
    private final int processId;

    private final DatagramSocket socket;
    private int nextMessageId = 0;
    private final HashMap<Integer, SendingMessage> sending = new HashMap<>();
    private final HashSet<Integer> removed = new HashSet<>();

    public PerfectLinkSender(int processId, DatagramSocket socket) {
        this.processId = processId;
        this.socket = socket;
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

    @Override
    public void run() {
        // if interrupt() is called during a Thread.sleep(),
        // an InterruptedException will be thrown but
        // isInterrupted() will continue to return false, this
        // is why an extra mechanism is needed
        boolean interrupted = false;

        long nextSendAttemptNanoTime = 0;

        while (true) {
            if (isInterrupted()) {
                interrupted = true;
            }

            synchronized (sending) {
                if (sending.isEmpty() && interrupted) {
                    System.out.println("PerfectLinkSender says GOODBYE");
                    break;
                }

                long nanoTime = System.nanoTime();
                if (nanoTime >= nextSendAttemptNanoTime) {
                    System.out.println("henlou");
                    long minLeftToWait = SendingMessage.TIMEOUT;
                    for (SendingMessage message : sending.values()) {
                        long leftToWait = message.trySend(nanoTime, socket);
                        if (leftToWait < minLeftToWait) {
                            minLeftToWait = leftToWait;
                        }
                    }
                    nextSendAttemptNanoTime = nanoTime + minLeftToWait;
                }

                // TODO: set global max buf size
                byte[] buf = new byte[256];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                // receive all the acknowledgements currently available
                int acksReceived = 0;
                while (true) {
                    try {
                        socket.receive(packet);
                        Message received = Message.received(packet);

                        if (received.messageType != Message.ACKNOWLEDGEMENT) {
                            throw new IllegalStateException("expected acknowledgement, got: " + received.messageType);
                        }

                        if (!removed.contains(received.messageId)) {
                            if (sending.remove(received.messageId) != null) {
                                removed.add(received.messageId);
                                acksReceived += 1;
                            } else {
                                throw new IllegalStateException("sender received a remove command for a message that has never existed: " + received.messageId);
                            }
                        }
                    } catch (SocketTimeoutException e) {
                        break;
                    } catch (IOException e) {
                        throw new Error(e);
                    }
                }

                System.out.println("acks received: " + acksReceived);
            }

            long sleepNanoTime = nextSendAttemptNanoTime - System.nanoTime();
            if (sleepNanoTime < 0) {
                continue;
            } else if (sleepNanoTime > 10_000_000 /* 10ms */) {
                sleepNanoTime = 10_000_000;
            }
            try {
                Thread.sleep(sleepNanoTime / 1_000_000);
//                wait(sleepNanoTime / 1_000_000);
            } catch (InterruptedException e) {
                interrupted = true;
                // the actual interruption check is done on the next iteration
            }
        }
    }
}
