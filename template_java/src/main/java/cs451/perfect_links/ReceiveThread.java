package cs451.perfect_links;

import cs451.base.BigEndianCoder;
import cs451.Constants;
import cs451.base.Pair;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ReceiveThread extends Thread {
    private final DatagramSocket socket;
    private final byte[] buffer = new byte[Constants.MAX_PACKET_SIZE];

    private final Consumer<DatagramPacket> normalPacketCallback;
    private final BiConsumer<Integer, Integer> acknowledgementCallback;

    // slow and broken
//    private final ExecutorService executor = Executors.newFixedThreadPool(Constants.PL_NUM_RECEIVER_THREADS);

    ReceiveThread(DatagramSocket socket, Consumer<DatagramPacket> normalPacketCallback, BiConsumer<Integer, Integer> acknowledgementCallback) {
        this.socket = socket;
        this.normalPacketCallback = normalPacketCallback;
        this.acknowledgementCallback = acknowledgementCallback;
    }

    private void handlePacket(DatagramPacket packet) {
        int packetId = BigEndianCoder.decodeInt(buffer, 0);
        int sourceId = BigEndianCoder.decodeInt(buffer, 4);
        if (packetId == 0) {
            int numAcks = BigEndianCoder.decodeInt(buffer, 8);
            for (int i = 0; i < numAcks; i++) {
                int acknowledgedPacketId = BigEndianCoder.decodeInt(buffer, i * 4 + 12);
                try {
                    acknowledgementCallback.accept(acknowledgedPacketId, sourceId);
                } catch (IllegalStateException e) {
                    System.out.println("Full packet:");
                    System.out.println(Arrays.toString(buffer));
                    throw e;
                }
            }
        } else {
            normalPacketCallback.accept(packet);
        }
    }

    @Override
    public void run() {
        while (!isInterrupted()) {
            try {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                handlePacket(packet);
            } catch (SocketTimeoutException ignored) {
            } catch (IOException e) {
                throw new Error(e);
            }
        }
    }
}
