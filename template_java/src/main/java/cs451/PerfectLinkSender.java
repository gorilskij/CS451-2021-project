package cs451;

import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.HashSet;

public class PerfectLinkSender extends Thread {
    private final int processId;

    private final DatagramSocket socket;
    private int nextMessageId = 0;
    private final HashMap<Integer, Message> sending = new HashMap<>();
    private final HashSet<Integer> removed = new HashSet<>();

    public PerfectLinkSender(int processId, DatagramSocket socket) {
        this.processId = processId;
        this.socket = socket;
    }

    public void send(String msg, FullAddress destination) {
        System.out.println("Enqueue \"" + msg + "\" to " + destination);

        int messageId = nextMessageId++;
        Message message = Message.normalMessage(messageId, processId, msg, destination);
        synchronized (sending) {
            sending.put(messageId, message);
        }
    }

    @Override
    public void run() {
        // if interrupt() is called during a Thread.sleep(),
        // an InterruptedException will be thrown but
        // isInterrupted() will continue to return false, this
        // is why an extra mechanism is needed
        boolean interrupted = false;

        while (true) {
            if (isInterrupted()) {
                interrupted = true;
            }

            synchronized (sending) {
                if (sending.isEmpty() && interrupted) {
                    System.out.println("PerfectLinkSender says GOODBYE");
                    break;
                }

                // TODO: do this less often
                for (Message message : sending.values()) {
                    try {
                        message.send(socket);
                    } catch (IOException e) {
                        throw new Error(e);
                    }
                }

                // TODO: set global max buf size
                byte[] buf = new byte[256];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);

                // receive all the acknowledgements currently available
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
            }

            // TODO: put send timers on all the messages separately
            //  or at least separate them from the ack listener timer
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                interrupted = true;
                // the actual interruption check is done on the next iteration
            }
        }
    }
}
