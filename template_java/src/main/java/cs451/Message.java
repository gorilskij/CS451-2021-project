package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

public class Message {
    // Messages can have the same id but different types,
    // these are different messages. The "primary key" for
    // identifying a message is (messageType, id).
    public final byte messageType;
    public final int messageId;
    public final int sourceId;
    public final String data;

    public final byte[] bytes;

    // destination address when sending, source address when receiving
    public final FullAddress address;

    // message types
    public static final byte NORMAL_MESSAGE = 1;
    public static final byte ACKNOWLEDGEMENT = 2;

    private static byte[] bytesFromData(byte messageType, int messageId, int sourceId, String data) {
        byte[] messageIdBytes = BigEndianCoder.encodeInt(messageId); // length 4
        byte[] sourceIdBytes = BigEndianCoder.encodeInt(sourceId); // length 4
        byte[] dataBytes = data.getBytes();

        byte[] bytes = new byte[1 + 4 + 4 + dataBytes.length];
        bytes[0] = messageType;
        System.arraycopy(messageIdBytes, 0, bytes, 1, 4);
        System.arraycopy(sourceIdBytes, 0, bytes, 1 + 4, 4);
        System.arraycopy(dataBytes, 0, bytes, 1 + 4 + 4, dataBytes.length);

        return bytes;
    }

    private Message(byte messageType, int messageId, int sourceId, String data, FullAddress address) {
        this.messageType = messageType;
        this.messageId = messageId;
        this.sourceId = sourceId;
        this.data = data;
        bytes = bytesFromData(messageType, messageId, sourceId, data);
        this.address = address;
    }

    private Message(DatagramPacket packet) {
        bytes = packet.getData();
        int packetLength = packet.getLength();

        messageType = bytes[0];
        messageId = BigEndianCoder.decodeInt(bytes, 1);
        sourceId = BigEndianCoder.decodeInt(bytes, 1 + 4);
        data = new String(bytes, 1 + 4 + 4, packetLength - 1 - 4 - 4);
        address = new FullAddress(packet.getAddress(), packet.getPort());
    }

    public static Message normalMessage(int messageId, int sourceId, String data, FullAddress address) {
        return new Message(NORMAL_MESSAGE, messageId, sourceId, data, address);
    }

    public static Message received(DatagramPacket packet) {
        return new Message(packet);
    }

    public void send(DatagramSocket socket) {
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, address.address, address.port);
        try {
            socket.send(packet);
        } catch (IOException ignore) {
            // try again later, a message won't be considered
            // sent until an acknowledgement is received so
            // failure to send is not a problem
        }
    }

    public void sendAcknowledgement(int sourceId, DatagramSocket socket) {
        if (messageType != NORMAL_MESSAGE) {
            throw new Error(new MessageTypeException(NORMAL_MESSAGE, messageType));
        }
        new Message(ACKNOWLEDGEMENT, messageId, sourceId, "", address).send(socket);
    }
}
