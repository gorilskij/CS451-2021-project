package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

public class Message {
    public final int id;
    public final String data;
    public final byte[] bytes;
    // destination address when sending, source address when receiving
    public final FullAddress address;

    // for sending
    public Message(int id, String data, FullAddress destination) {
        this.id = id;
        this.data = data;
        byte[] idBytes = BigEndianCoder.encodeInt(id);
        byte[] dataBytes = data.getBytes();
        bytes = new byte[idBytes.length + dataBytes.length];
        System.arraycopy(idBytes, 0, bytes, 0, 4);
        System.arraycopy(dataBytes, 0, bytes, 4, dataBytes.length);
        address = destination;
    }

    // for receiving
    public Message(DatagramPacket packet) {
        bytes = packet.getData();
        int packetLength = packet.getLength();

        byte[] idBytes = new byte[4];
        System.arraycopy(bytes, 0, idBytes, 0, 4);
        id = BigEndianCoder.decodeInt(idBytes);
        data = new String(bytes, 4, packetLength - 4);
        address = new FullAddress(packet.getAddress(), packet.getPort());
    }

    public void send(DatagramSocket socket) throws IOException {
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, address.address, address.port);
        socket.send(packet);
    }
}
