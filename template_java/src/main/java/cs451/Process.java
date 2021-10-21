package cs451;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public abstract class Process extends Thread {
    public final int id;

    Process(int id) {
        this.id = id;
    }
}


