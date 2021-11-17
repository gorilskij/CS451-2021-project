package cs451.perfect_links;

import cs451.base.BigEndianCoder;

import java.util.Iterator;

public class Packet implements Iterable<MessageFragment> {
    // normal packet format:
    //   packet id (4 bytes) (> 0)
    //   source id (4 bytes)
    //   (message fragments)+ (each with its own metadata)

    // acknowledgement format:
    //   0 (4 bytes)
    //   source id (4 bytes) (receiver of the original packets)
    //   (acknowledged packet ids)+ (4 bytes each)

    public static final int METADATA_SIZE = 8;

    public final int packetId;
    public final int sourceId;
    public final byte[] bytes; // includes packetId
    private final int length; // how much of bytes is valid

    public Packet(byte[] bytes, int length) {
        this.packetId = BigEndianCoder.decodeInt(bytes, 0);
        this.sourceId = BigEndianCoder.decodeInt(bytes, 4);
        this.bytes = bytes;
        this.length = length;
    }

    public Packet(byte[] bytes) {
        this(bytes, bytes.length);
    }

    @Override
    public Iterator<MessageFragment> iterator() {
        return new PacketFragmentIterator(bytes, length);
    }
}
