package cs451.perfect_links;

import java.util.HashMap;
import java.util.stream.IntStream;

// recombines a message from message fragments
public class MessageBuilder {
    int expectedFragments = -1;
    private final HashMap<Integer, MessageFragment> fragments = new HashMap<>();

    private final int messageId;
    private final int sourceId;

    MessageBuilder(int messageId, int sourceId) {
        this.messageId = messageId;
        this.sourceId = sourceId;
    }

    public void add(MessageFragment fragment) {
        fragments.put(fragment.fragmentIdx, fragment);

        if (fragment.isLast) {
            expectedFragments = fragment.fragmentIdx + 1;
        }
    }

    // returns null if not enough fragments are present yet
    public PLMessage tryBuild() {
        if (fragments.size() == expectedFragments) {
            int length = IntStream
                    .range(0, expectedFragments)
                    .map(i -> fragments.get(i).data.length)
                    .sum();

            byte[] bytes = new byte[length];
            int current = 0;
            for (int i = 0; i < expectedFragments; i++) {
                byte[] data = fragments.get(i).data;
                System.arraycopy(data, 0, bytes, current, data.length);
                current += data.length;
            }

            return new PLMessage(messageId, sourceId, bytes);
        }

        return null;
    }
}
