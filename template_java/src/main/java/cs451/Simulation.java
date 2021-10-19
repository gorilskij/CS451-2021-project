package cs451;

import java.net.InetAddress;

public class Simulation extends Thread {
    private int id;

    private boolean sender;
    // sender-specific
    private int leftToSend;
    private InetAddress destinationAddress;

    // receiver
    public Simulation(int id) {
        this.id = id;
        this.sender = false;
    }

    // sender
    public Simulation(int id, int messagesToSend, InetAddress destinationAddress) {
        this.id = id;
        this.sender = true;
        this.leftToSend = messagesToSend;
        this.destinationAddress = destinationAddress;
    }

    @Override
    public void run() {
        
    }
}
