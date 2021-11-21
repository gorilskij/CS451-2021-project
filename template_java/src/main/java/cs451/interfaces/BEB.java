package cs451.interfaces;

import java.util.Set;

public interface BEB {
    void bebBroadcast(byte[] msgBytes);
    void bebBroadcast(byte[] msgBytes, Set<Integer> excludeIds);
    void close();
}
