package cs451;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.HashSet;

public class PerfectLinkSender extends Thread {
    private final int SEND_TO_PORT = 4445;

    private DatagramSocket socket;
    private int nextMessageId = 0;
    private ArrayList<Message> sending = new ArrayList<>();
    private HashSet<Integer> removed = new HashSet<>();
    private boolean running = false;

    public PerfectLinkSender() {
        try {
            socket = new DatagramSocket();
            socket.setSoTimeout(10);
        } catch (SocketException e) {
            throw new Error(e);
        }
    }

    public void send(String msg, InetAddress address) {
        Message message = new Message(nextMessageId++, msg, address, SEND_TO_PORT);
        sending.add(message);
    }

    @Override
    public void run() {
        running = true;
        while (running) {
            if (sending.isEmpty()) {
                try {
                    Thread.sleep(30);
                } catch (InterruptedException e) {
                    running = false;
                    break;
                }
            } else {
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
                } catch (SocketTimeoutException e) {
                    // nothing received
                } catch (IOException e) {
                    throw new Error(e);
                }
                Message received = new Message(buf, packet.getLength());
                if (!removed.contains(received.id)) {
                    if (sending.removeIf(m -> m.id == received.id)) {
                        removed.add(received.id);
                    } else {
                        throw new IllegalStateException("sender received a remove command for a message that has never existed");
                    }
                }
            }
        }
    }
}
