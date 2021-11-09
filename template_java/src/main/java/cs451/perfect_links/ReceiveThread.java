package cs451.perfect_links;

import cs451.base.BigEndianCoder;
import cs451.Constants;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.function.Consumer;

public class ReceiveThread extends Thread {
    private final DatagramSocket socket;
    private final byte[] buffer = new byte[Constants.MAX_PACKET_SIZE];

    private final Consumer<DatagramPacket> normalPacketCallback;
    private final Consumer<Integer> acknowledgementCallback;

    ReceiveThread(DatagramSocket socket, Consumer<DatagramPacket> normalPacketCallback, Consumer<Integer> acknowledgementCallback) {
        this.socket = socket;
        this.normalPacketCallback = normalPacketCallback;
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

            int packetId = BigEndianCoder.decodeInt(buffer, 0);
            if (packetId == 0) {
                int acknowledgedPacketId = BigEndianCoder.decodeInt(buffer, 8);
                acknowledgementCallback.accept(acknowledgedPacketId);
            } else {
                normalPacketCallback.accept(packet);
            }
        }
    }
}
