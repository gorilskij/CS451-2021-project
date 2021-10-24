package cs451.perfectLinks;

import cs451.BigEndianCoder;

// a packetId of 0 indicates an acknowledgement
// in that case, the first 4 bytes are the packetId
// that is being acknowledged
public class Packet {
    public final int packetId; // TODO: redundant?
    public final byte[] data; // includes packetId

    public Packet(int packetId, byte[] data) {
        this.packetId = packetId;
        this.data = data;
    }

    // normal packet format:
    // packet id (4 bytes) (> 0)
    // source id (4 bytes)
    // (message fragments)+ (each with its own metadata)

    // acknowledgement format:
    // 0 (4 bytes)
    // source id (4 bytes) (receiver of the original packet)
    // acknowledged packet id (4 bytes)
    public Packet acknowledgement(int sourceId) {
        byte[] newData = new byte[12]; // initialized with 0
        BigEndianCoder.encodeInt(sourceId, newData, 4);
        BigEndianCoder.encodeInt(packetId, newData, 8);
        return new Packet(0, newData);
    }
}
