package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;

public class PerfectLinkReceiver extends Thread {
    private final DatagramSocket socket;
    private byte[] buf = new byte[256];

    private final ArrayList<String> receivedQueue = new ArrayList<>();
    private final HashMap<Integer, FullAddress> receivedIds = new HashMap<>();

    public PerfectLinkReceiver(DatagramSocket socket) {
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

            Message received = new Message(packet);

            if (receivedIds.containsKey(received.id)) {
                FullAddress destination = receivedIds.get(received.id);
                try {
                    new Message(received.id, "", destination).send(socket);
                } catch (IOException e) {
                    /* ignore, try again next iteration */
                }
            } else {
                receivedIds.put(received.id, received.address);
                synchronized (receivedQueue) {
                    receivedQueue.add(received.data);
                }
            }
        }
    }

    public String receive() {
        synchronized (receivedQueue) {
            if (receivedQueue.size() == 0) {
                return null;
            } else {
                String ret = receivedQueue.get(0);
                receivedQueue.remove(0);
                return ret;
            }
        }
    }
}
