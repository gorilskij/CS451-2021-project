package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Main {
    private static final EventHistory eventHistory = new EventHistory();
    private static String outputFilePath;

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");
        // TODO: separate immediate termination mechanism

        //write/flush output file if necessary
        System.out.println("Writing output.");
        eventHistory.writeToFile(outputFilePath);
    }

    private static void initSignalHandlers() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                handleSignal();
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        Parser parser = new Parser(args);
        parser.parse();

        initSignalHandlers();

        // example
        long pid = ProcessHandle.current().pid();
        System.out.println("My PID: " + pid + "\n");
        System.out.println("From a new terminal type `kill -SIGINT " + pid + "` or `kill -SIGTERM " + pid + "` to stop processing packets\n");

        System.out.println("My ID: " + parser.myId() + "\n");
        System.out.println("List of resolved hosts is:");
        System.out.println("==========================");
        for (Host host : parser.hosts()) {
            System.out.println(host.getId());
            System.out.println("Human-readable IP: " + host.getIp());
            System.out.println("Human-readable Port: " + host.getPort());
            System.out.println();
        }
        System.out.println();

        System.out.println("Path to output:");
        System.out.println("===============");
        System.out.println(parser.output() + "\n");
        outputFilePath = parser.output();

        System.out.println("Path to config:");
        System.out.println("===============");
        System.out.println(parser.config() + "\n");

        System.out.println("Doing some initialization\n");
        int numMessages;
        int receiverId;
        try {
            String config = Files.readString(Paths.get(parser.config())).stripTrailing();
            String[] components = config.split(" ");
            numMessages = Integer.parseInt(components[0]);
            receiverId = Integer.parseInt(components[1]);
        } catch (IOException e) {
            throw new Error(e);
        }


        // TODO: redo
        InetAddress receiverAddress = null;
        int receiverPort = -1;
        int myPort = -1;
        for (Host host : parser.hosts()) {
            if (host.getId() == receiverId) {
                try {
                    receiverAddress = InetAddress.getByName(host.getIp());
                } catch (UnknownHostException e) {
                    throw new Error(e);
                }
                receiverPort = host.getPort();
            }

            if (host.getId() == parser.myId()) {
                myPort = host.getPort();
            }

            // TODO: improve this find thing
            if (receiverPort > -1 && myPort > -1) {
                break;
            }
        }

        FullAddress receiverFullAddress = new FullAddress(receiverAddress, receiverPort);

        DatagramSocket socket;
        try {
            socket = new DatagramSocket(myPort);
            socket.setSoTimeout(10);
        } catch (SocketException e) {
            throw new Error(e);
        }

        PerfectLink perfectLink = new PerfectLink(parser.myId(), socket);

        System.out.println("Broadcasting and delivering messages...\n");

        boolean isReceiver = parser.myId() == receiverId;

        // send
        if (!isReceiver) {
            for (int i = 0; i < numMessages; i++) {
                perfectLink.send("message " + i + " from process " + parser.myId(), receiverFullAddress);
                eventHistory.logBroadcast(i);
            }
        }

        long start = System.nanoTime();
        int expectedMsgs = (parser.hosts().size() - 1) * numMessages;
        int totalMsgs = 0;

        HashMap<Integer, Integer> totals = new HashMap<>();

        // receive (forever)
        outer:
        while (true) {
            Message delivered;
            while ((delivered = perfectLink.tryDeliver()) != null) {
                totalMsgs += 1;

                if (!totals.containsKey(delivered.sourceId)) {
                    totals.put(delivered.sourceId, 0);
                }
                totals.put(delivered.sourceId, totals.get(delivered.sourceId) + 1);

                System.out.println("DELIVER \"" + delivered.text + "\"");
                System.out.println("  total: " + totalMsgs);

                for (Map.Entry<Integer, Integer> entry : totals.entrySet()) {
                    System.out.println(entry.getValue() + " from " + entry.getKey());
                }

                eventHistory.logDelivery(delivered.sourceId, delivered.messageId);

                if (totalMsgs == expectedMsgs) {
                    break outer;
                }
            }
        }

        long end = System.nanoTime();
        if (isReceiver) {
            System.out.println("total number of messages received: " + totalMsgs);
            System.out.println("time taken: " + (end - start) / 1_000_000 + "ms");
            System.out.println("messages/s: " + ((long) (totalMsgs * 1e9) / (end - start)));
        }

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
    }
}
