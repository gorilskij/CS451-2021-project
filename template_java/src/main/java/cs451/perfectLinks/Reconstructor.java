package cs451.perfectLinks;

import cs451.base.BigEndianCoder;
import cs451.base.Message;
import cs451.base.Pair;

import java.net.DatagramPacket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.function.Consumer;

/**
 * Reconstructs messages from fragments as they arrive, fragments can arrive in any order, any
 * number of times, when a message is ready, it's passed to `deliverCallback`
 */
public class Reconstructor {
    private final Consumer<Message> deliverCallback;
    // indexed by (messageId, sourceId)
    private final HashMap<Pair<Integer, Integer>, MessageBuilder> builders = new HashMap<>();
    // contains (messageId, sourceId)

    public Reconstructor(Consumer<Message> deliverCallback) {
        this.deliverCallback = deliverCallback;
    }

    public void add(byte[] packetData, int packetLength) {
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
                deliverCallback.accept(message);
                builders.remove(key);
            }
        }
    }
}
