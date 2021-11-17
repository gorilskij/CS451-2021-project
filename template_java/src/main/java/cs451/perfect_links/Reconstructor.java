package cs451.perfect_links;

import cs451.base.Pair;
import cs451.message.PLMessage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Reconstructs messages from fragments as they arrive, fragments can arrive in any order, any
 * number of times, when a message is ready, it's passed to `deliverCallback`
 */
public class Reconstructor {
    private final Consumer<PLMessage> deliver;
    // indexed by (messageId, sourceId)
    private final Map<Pair<Integer, Integer>, MessageBuilder> builders = new ConcurrentHashMap<>();
    // contains (messageId, sourceId)

    public Reconstructor(Consumer<PLMessage> deliver) {
        this.deliver = deliver;
    }

    public void add(byte[] packetData, int packetLength) {
        Packet packet = new Packet(packetData, packetLength);

        for (MessageFragment fragment : packet) {
            Pair<Integer, Integer> key = new Pair<>(fragment.messageId, packet.sourceId);
            PLMessage message = builders
                    .computeIfAbsent(key, ignored -> new MessageBuilder(fragment.messageId, packet.sourceId))
                    .add(fragment)
                    .tryBuild();

            if (message != null) {
                deliver.accept(message);
                builders.remove(key);
            }
        }
    }
}
