package cs451.uniform_reliable_broadcast;

import cs451.message.URBMessage;

import java.util.HashSet;
import java.util.Set;

class AckCounter {
    final URBMessage message;
    Set<Integer> acknowledged = new HashSet<>();

    AckCounter(URBMessage message) {
        this.message = message;
    }
}
