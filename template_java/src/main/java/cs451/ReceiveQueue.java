package cs451;

import java.net.DatagramPacket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

// recombines a message from message fragments
class MessageBuilder {
    int expectedFragments = -1;
    private final HashMap<Integer, MessageFragment> fragments = new HashMap<>();
    private final int sourceId;

    int messageId = -1;

    MessageBuilder(int sourceId) {
        this.sourceId = sourceId;
    }

    public void add(MessageFragment fragment) {
        if (messageId < 0) {
            messageId = fragment.messageId;
        }

        fragments.put(fragment.fragmentIdx, fragment);

        if (fragment.isLast) {
            expectedFragments = fragment.fragmentIdx + 1;
        }
    }

    // returns null if not enough fragments are present yet
    public Message tryBuild() {
        if (fragments.size() == expectedFragments) {
            StringBuilder text = new StringBuilder();

            for (int i = 0; i < expectedFragments; i++) {
                byte[] bytes = fragments.get(i).data;
                text.append(new String(bytes));
            }

            return new Message(messageId, text.toString(), sourceId);
        }

        return null;
    }
}

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

            if (!builders.containsKey(key)) {
                builders.put(key, new MessageBuilder(sourceId));
            }

            MessageBuilder builder = builders.get(key);
            builder.add(fragment);
            Message message = builder.tryBuild();

            if (message != null) {
                synchronized (delivered) {
                    delivered.add(message);
                }
                builders.remove(key);

//                if (builders.isEmpty()) {
//                    System.out.println("BUILDERS EMPTY");
//                } else {
//                    System.out.println(builders.size() + "BUILDERS REMAINING");
//                }
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
