package cs451.fifo;

import cs451.base.FullAddress;
import cs451.interfaces.FIFO;
import cs451.message.FIFOMessage;
import cs451.interfaces.URB;
import cs451.uniform_reliable_broadcast.URBImpl;

import java.net.DatagramSocket;
import java.util.Comparator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.function.Consumer;

class DeliveryQueue {
    // FIFO enforcement
    private int nextDeliveryId = 1;
    private final Queue<FIFOMessage> queue = new PriorityBlockingQueue<>(100, Comparator.comparing(FIFOMessage::getFifoMessageId));
    private final Consumer<FIFOMessage> deliverCallback;

    DeliveryQueue(Consumer<FIFOMessage> deliverCallback) {
        super();
        this.deliverCallback = deliverCallback;
    }

    public synchronized void deliver(FIFOMessage message) {
        if (message.getFifoMessageId() == nextDeliveryId) {
            deliverCallback.accept(message);
            nextDeliveryId++;
            while (!queue.isEmpty() && queue.peek().getFifoMessageId() == nextDeliveryId) {
                deliverCallback.accept(queue.poll());
                nextDeliveryId++;
            }
        } else {
            queue.offer(message);
        }
    }
}

public class FIFOImpl implements FIFO {
    private final URB urb;
    // priority queues containing (urbMessageId, message) sorted by urbMessageId, messages waiting to be delivered in order, indexed by urbSourceId
    private final Map<Integer, DeliveryQueue> deliveryQueues = new ConcurrentHashMap<>();

    public FIFOImpl(int processId, Map<Integer, FullAddress> addresses, DatagramSocket socket, Consumer<FIFOMessage> deliverCallback) {
        this.urb = new URBImpl(processId, addresses, socket, urbMessage -> deliveryQueues
                .computeIfAbsent(urbMessage.getUrbSourceId(), ignored -> new DeliveryQueue(deliverCallback))
                .deliver(new FIFOMessage(urbMessage)));
    }

    @Override
    public void fifoBroadcast(String msg) {
        urb.urbBroadcast(msg);
    }

    @Override
    public void close() {
        urb.close();
    }
}
