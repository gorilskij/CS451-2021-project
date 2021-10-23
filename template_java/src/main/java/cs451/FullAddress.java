package cs451;

import java.net.InetAddress;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FullAddress that = (FullAddress) o;
        return address == that.address && port == that.port;
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port);
    }
}
