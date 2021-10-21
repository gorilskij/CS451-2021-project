package cs451;

import java.net.InetAddress;

public final class FullAddress {
    public final InetAddress address;
    public final int port;

    public FullAddress(InetAddress address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public String toString() {
        return address + ":" + port;
    }
}
