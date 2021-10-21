package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

public class PerfectLinkReceiver extends Thread {
    private final int processId;

    private final DatagramSocket socket;
    private byte[] buf = new byte[256];

    private final ArrayList<Message> receivedQueue = new ArrayList<>();
    // TODO: convert back to HashSet
    private final HashSet<MessageKey> receivedIds = new HashSet<>();

    private static class MessageKey {
        public final int messageId;
        public final int sourceId;

        private MessageKey(int messageId, int sourceId) {
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

    public PerfectLinkReceiver(int processId, DatagramSocket socket) {
        this.processId = processId;
        this.socket = socket;
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            
            try {
                socket.receive(packet);
            } catch (SocketTimeoutException e) {
                continue;
            } catch (IOException e) {
                // TODO: improve error handling
                throw new Error(e);
            }

            Message received = Message.received(packet);

            if (received.messageType != Message.NORMAL_MESSAGE) {
                // ignore acknowledgement messages
                continue;
            }

            MessageKey key = new MessageKey(received.messageId, received.sourceId);
            if (receivedIds.contains(key)) {
                received.sendAcknowledgement(processId, socket);
            } else {
                receivedIds.add(key);
                synchronized (receivedQueue) {
                    receivedQueue.add(received);
                }
            }
        }
    }

    public Message deliver() {
        synchronized (receivedQueue) {
            if (receivedQueue.size() == 0) {
                return null;
            } else {
                return receivedQueue.remove(0);
            }
        }
    }
}
