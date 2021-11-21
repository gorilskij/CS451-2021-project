package cs451.interfaces;

public interface PerfectLink {
    void plSend(String msg, Integer destinationId);
    void plSend(byte[] msgBytes, Integer destinationId);
    void close();
}
