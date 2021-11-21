package cs451.fifo;

import cs451.base.FullAddress;
import cs451.message.FIFOMessage;
import cs451.uniform_reliable_broadcast.URB;

import java.net.DatagramSocket;
import java.util.Comparator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

class DeliveryQueue {
    // FIFO enforcement
    private int nextDeliveryId = 0;
    private final Queue<FIFOMessage> queue = new PriorityBlockingQueue<>(100, Comparator.comparing(message -> message.messageId));
    private final Consumer<FIFOMessage> deliverCallback;

    DeliveryQueue(Consumer<FIFOMessage> deliverCallback) {
        super();
        this.deliverCallback = deliverCallback;
    }

    public synchronized void deliver(FIFOMessage message) {
        if (message.messageId == nextDeliveryId) {
            deliverCallback.accept(message);
            nextDeliveryId++;
            while (!queue.isEmpty() && queue.peek().messageId == nextDeliveryId) {
                deliverCallback.accept(queue.poll());
                nextDeliveryId++;
            }
        } else {
            queue.offer(message);
        }
    }
}

public class FIFO {
    private final URB urb;
    // priority queues containing (urbMessageId, message) sorted by urbMessageId, messages waiting to be delivered in order, indexed by urbSourceId
    private final Map<Integer, DeliveryQueue> deliveryQueues = new ConcurrentHashMap<>();

    public FIFO(int processId, Map<Integer, FullAddress> addresses, DatagramSocket socket, Consumer<FIFOMessage> deliverCallback) {
        this.urb = URB.newURB(processId, addresses, socket, urbMessage -> deliveryQueues
                .computeIfAbsent(urbMessage.sourceId, ignored -> new DeliveryQueue(deliverCallback))
                .deliver(new FIFOMessage(urbMessage)));
    }

    public synchronized void broadcast(String msg) {
        urb.broadcast(msg);
    }

    public void close() {
        urb.close();
    }
}
