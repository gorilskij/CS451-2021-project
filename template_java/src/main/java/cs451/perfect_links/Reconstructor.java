package cs451.perfect_links;

import cs451.base.BigEndianCoder;
import cs451.base.Pair;
import cs451.message.PLMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Reconstructs messages from fragments as they arrive, fragments can arrive in any order, any
 * number of times, when a message is ready, it's passed to `deliverCallback`
 */
public class Reconstructor {
    private final Consumer<PLMessage> deliverCallback;
    // indexed by (messageId, sourceId)
    private final Map<Pair<Integer, Integer>, MessageBuilder> builders = new ConcurrentHashMap<>();
    // contains (messageId, sourceId)

    public Reconstructor(Consumer<PLMessage> deliverCallback) {
        this.deliverCallback = deliverCallback;
    }

//    private static final AtomicInteger counter = new AtomicInteger(0);
    public void add(byte[] packetData, int packetLength) {
//        System.out.println("add call number: " + counter.incrementAndGet());

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

            PLMessage message = builder.tryBuild();
            if (message != null) {
                deliverCallback.accept(message);
                builders.remove(key);
            }
        }
    }
}
