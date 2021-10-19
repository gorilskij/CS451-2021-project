package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class Message {
    public final int id;
    public final String data;
    public final byte[] bytes;
    // only used when sending
    public final InetAddress address;
    public final int port;

    // for sending
    public Message(int id, String data, InetAddress address, int port) {
        this.id = id;
        this.data = data;
        byte[] idBytes = BigEndianCoder.encodeInt(id);
        byte[] dataBytes = data.getBytes();
        bytes = new byte[idBytes.length + dataBytes.length];
        System.arraycopy(idBytes, 0, bytes, 0, 4);
        System.arraycopy(dataBytes, 0, bytes, 4, dataBytes.length);
        this.address = address;
        this.port = port;
    }

    // for receiving
    public Message(byte[] bytes, int packetLength) {
        this.bytes = bytes;
        byte[] idBytes = new byte[4];
        System.arraycopy(bytes, 0, idBytes, 0, 4);
        id = BigEndianCoder.decodeInt(idBytes);
        data = new String(bytes, 4, packetLength - 4);
        this.address = null;
        this.port = -1;
    }

    public void send(DatagramSocket socket) throws IOException {
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, address, port);
        socket.send(packet);
    }
}
