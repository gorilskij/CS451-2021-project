package cs451.perfect_links;

import java.util.Iterator;

public class PacketFragmentIterator implements Iterator<MessageFragment> {
    private final byte[] bytes;
    private final int length;
    private int index = Packet.METADATA_SIZE;

    PacketFragmentIterator(byte[] bytes, int length) {
        this.bytes = bytes;
        this.length = length;
    }

    @Override
    public boolean hasNext() {
        return index < length;
    }

    @Override
    public MessageFragment next() {
        MessageFragment fragment = new MessageFragment(bytes, index);
        index += fragment.size();
        return fragment;
    }
}
