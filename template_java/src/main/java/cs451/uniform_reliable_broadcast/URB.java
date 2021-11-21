package cs451.uniform_reliable_broadcast;

import cs451.base.FullAddress;
import cs451.message.URBMessage;

import java.net.DatagramSocket;
import java.util.*;
import java.util.function.Consumer;

public abstract class URB {
    public static URB newURB(int processId, Map<Integer, FullAddress> addresses, DatagramSocket socket, Consumer<URBMessage> deliverCallback) {
        return new URBImpl(processId, addresses, socket, deliverCallback);
    }

    public abstract void broadcast(String msg);
    public abstract void close();
}
