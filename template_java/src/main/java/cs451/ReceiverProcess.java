package cs451;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.ArrayList;

public class ReceiverProcess extends Process {
    private final PerfectLinkReceiver perfectLinkReceiver;

    private final ArrayList<String> receivedMessages = new ArrayList<>();

    // just receiver
    public ReceiverProcess(int id, DatagramSocket socket) {
        super(id);
        perfectLinkReceiver = new PerfectLinkReceiver(id, socket);
        perfectLinkReceiver.start();
    }

    @Override
    public void run() {
        while (true) {
            System.out.println("trying perfectLinkReceiver.receive()");
            String received = perfectLinkReceiver.receive();
            if (received == null) {
                System.out.println("> null");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            } else {
                System.out.println("> yay, got \"" + received + "\"");
                receivedMessages.add(received);
            }
        }
    }
}
