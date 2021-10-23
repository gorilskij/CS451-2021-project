package cs451;

import java.net.DatagramPacket;
import java.util.ArrayList;
import java.util.HashMap;

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

public class ReceiveQueue {
    private final ArrayList<Message> delivered = new ArrayList<>();
    private final HashMap<Integer, MessageBuilder> builders = new HashMap<>();

    public void add(DatagramPacket packet) {
        byte[] packetData = packet.getData();
        int packetLength = packet.getLength();

        int sourceId = BigEndianCoder.decodeInt(packetData, 4);

        // skip packet metadata
        for (int currentIdx = 8; currentIdx < packetLength;) {
            MessageFragment fragment = new MessageFragment(packetData, currentIdx);
            currentIdx += fragment.size();

            if (!builders.containsKey(fragment.messageId)) {
                builders.put(fragment.messageId, new MessageBuilder(sourceId));
            }

            MessageBuilder builder = builders.get(fragment.messageId);
            builder.add(fragment);
            Message message = builder.tryBuild();

            if (message != null) {
                delivered.add(message);
                builders.remove(fragment.messageId);

                if (builders.isEmpty()) {
                    System.out.println("BUILDERS EMPTY");
                } else {
                    System.out.println(builders.size() + "BUILDERS REMAINING");
                }
            }
        }
    }

    public Message tryDeliver() {
        if (delivered.size() > 0) {
            return delivered.remove(0);
        }
        return null;
    }
}
