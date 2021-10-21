package cs451;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;

public class SenderProcess extends Process {
    private final PerfectLinkSender perfectLinkSender;
    private int leftToSend;
    private int nextMessageIndex = 0;

    private final ArrayList<Integer> sentMessages = new ArrayList<>();

    public SenderProcess(int id, DatagramSocket socket, int messagesToSend, FullAddress destination) {
        super(id);
        this.leftToSend = messagesToSend;
        perfectLinkSender = new PerfectLinkSender(socket, destination);
        perfectLinkSender.start();
    }

    @Override
    public void run() {
        // TODO: convert to a for loop
        while (leftToSend > 0) {
            int index = nextMessageIndex++;
            perfectLinkSender.send("message " + index + " from process " + id);
            sentMessages.add(index);
            leftToSend--;
        }

        perfectLinkSender.interrupt();
        try {
            perfectLinkSender.join();
        } catch (InterruptedException e) {
            /* return */
        }
    }
}
