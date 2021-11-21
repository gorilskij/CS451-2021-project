package cs451.best_effort_broadcast;

import cs451.base.BigEndianCoder;
import cs451.base.FullAddress;
import cs451.interfaces.BEB;
import cs451.interfaces.PerfectLink;
import cs451.message.PLMessage;
import cs451.perfect_links.PerfectLinkImpl;

import java.net.DatagramSocket;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class BEBImpl implements BEB {
    private final int processId;
    private final int totalNumProcesses;

    private final PerfectLink perfectLink;

    // expects processes to be numbered sequentially starting from 1
    public BEBImpl(int processId, Map<Integer, FullAddress> addresses, DatagramSocket socket, Consumer<PLMessage> deliver) {
        this.processId = processId;
        totalNumProcesses = addresses.size();
        perfectLink = new PerfectLinkImpl(processId, addresses, socket, deliver);
    }

    @Override
    public void bebBroadcast(byte[] bytes) {
        for (int destinationId = 1; destinationId <= totalNumProcesses; destinationId++) {
            if (destinationId != processId) {
                perfectLink.plSend(bytes, destinationId);
            }
        }
    }

//    private void bebBroadcast(byte[] bytes, Set<Integer> exclude) {
//        for (int destinationId = 1; destinationId <= totalNumProcesses; destinationId++) {
//            if (destinationId != processId && !exclude.contains(destinationId)) {
//                perfectLink.plSend(bytes, destinationId);
//            }
//        }
//    }


    @Override
    public void close() {
        perfectLink.close();
    }
}
