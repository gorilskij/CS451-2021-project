package cs451.perfectLinks;

import cs451.BigEndianCoder;
import cs451.Message;
import cs451.Pair;

import java.net.DatagramPacket;
import java.util.HashMap;
import java.util.Objects;
import java.util.function.Consumer;

public class ReceiveQueue {
//    private final ArrayList<Message> delivered = new ArrayList<>();
    private final Consumer<Message> deliverCallback;
    // indexed by (messageId, sourceId)
    private final HashMap<Pair<Integer, Integer>, MessageBuilder> builders = new HashMap<>();

    public ReceiveQueue(Consumer<Message> deliverCallback) {
        this.deliverCallback = deliverCallback;
    }

    public void add(DatagramPacket packet) {
        byte[] packetData = packet.getData();
        int packetLength = packet.getLength();

        int sourceId = BigEndianCoder.decodeInt(packetData, 4);

        // skip packet metadata
        for (int currentIdx = 8; currentIdx < packetLength;) {
            MessageFragment fragment = new MessageFragment(packetData, currentIdx);
            currentIdx += fragment.size();

            Pair<Integer, Integer> key = new Pair<>(fragment.messageId, sourceId);

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
                synchronized (deliverCallback) {
                    deliverCallback.accept(message);
                }
                builders.remove(key);
            }
        }
    }
}
