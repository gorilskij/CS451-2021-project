package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

public class PerfectLinkReceiver extends Thread {
    private final DatagramSocket socket;
    private boolean running = false;
    private byte[] buf = new byte[256];

    public PerfectLinkReceiver(DatagramSocket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        running = true;
        while (running) {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            
            try {
                socket.receive(packet);
            } catch (IOException e) {
                // TODO: improve error handling
                throw new Error(e);
            }

            String received = new String(packet.getData(), packet.getOffset(), packet.getLength());
            
        }
    }
}
