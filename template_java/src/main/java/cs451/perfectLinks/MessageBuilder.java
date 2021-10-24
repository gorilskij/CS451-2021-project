package cs451.perfectLinks;

import cs451.base.Message;
import java.util.HashMap;

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
