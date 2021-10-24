package cs451.perfectLinks;

import cs451.BigEndianCoder;
import cs451.Message;

import java.net.DatagramPacket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

class MessageKey {
    public final int messageId;
    public final int sourceId;

    MessageKey(int packetId, int sourceId) {
        this.messageId = packetId;
        this.sourceId = sourceId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageKey that = (MessageKey) o;
        return messageId == that.messageId && sourceId == that.sourceId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, sourceId);
    }
}

public class ReceiveQueue {
    private final ArrayList<Message> delivered = new ArrayList<>();
    private final HashMap<MessageKey, MessageBuilder> builders = new HashMap<>();

    public void add(DatagramPacket packet) {
        byte[] packetData = packet.getData();
        int packetLength = packet.getLength();

        int sourceId = BigEndianCoder.decodeInt(packetData, 4);

        // skip packet metadata
        for (int currentIdx = 8; currentIdx < packetLength;) {
            MessageFragment fragment = new MessageFragment(packetData, currentIdx);
            currentIdx += fragment.size();

            MessageKey key = new MessageKey(fragment.messageId, sourceId);

            MessageBuilder builder;
            if (!builders.containsKey(key)) {
                builder = new MessageBuilder(fragment.messageId, sourceId);
                builders.put(key, builder);
            } else {
                builder = builders.get(key);
            }
            builder.add(fragment);

            Message message = builder.tryBuild();
            if (message != null) {
                synchronized (delivered) {
                    delivered.add(message);
                }
                builders.remove(key);
            }
        }
    }

    public Message tryDeliver() {
        synchronized (delivered) {
            if (delivered.size() > 0) {
                return delivered.remove(0);
            }
        }
        return null;
    }
}
