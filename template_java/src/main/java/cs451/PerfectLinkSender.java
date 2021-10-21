package cs451;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.HashSet;

public class PerfectLinkSender extends Thread {
    private final int processId;

    private final DatagramSocket socket;
    private int nextMessageId = 0;
    private final ArrayList<Message> sending = new ArrayList<>();
    private final HashSet<Integer> removed = new HashSet<>();

    private final FullAddress destination;

    public PerfectLinkSender(int processId, DatagramSocket socket, FullAddress destination) {
        this.processId = processId;
        this.destination = destination;
        this.socket = socket;
    }

    public void send(String msg) {
        System.out.println("Enqueue \"" + msg + "\" to " + destination);

        Message message = Message.normalMessage(nextMessageId++, processId, msg, destination);
        synchronized (sending) {
            sending.add(message);
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
                for (Message message : sending) {
                    try {
                        message.send(socket);
                    } catch (IOException e) {
                        throw new Error(e);
                    }
                }

                // TODO: set global max buf size
                byte[] buf = new byte[256];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                try {
                    socket.receive(packet);
                    Message received = Message.received(packet);

                    if (received.messageType != Message.ACKNOWLEDGEMENT) {
                        throw new IllegalStateException("expected acknowledgement, got: " + received.messageType);
                    }

                    if (!removed.contains(received.messageId)) {
                        if (sending.removeIf(m -> m.messageId == received.messageId)) {
                            removed.add(received.messageId);
                            System.out.println("removed " + received.messageId + ", new length: " + sending.size());
                        } else {
                            throw new IllegalStateException("sender received a remove command for a message that has never existed: " + received.messageId);
                        }
                    }
                } catch (SocketTimeoutException e) {
                    // nothing received
                } catch (IOException e) {
                    throw new Error(e);
                }
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                interrupted = true;
                // the actual interruption check is done on the next iteration
            }
        }
    }
}
