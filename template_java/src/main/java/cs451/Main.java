package cs451;

import cs451.base.FullAddress;
import cs451.fifo.FIFOImpl;
import cs451.interfaces.FIFO;
import cs451.interfaces.PerfectLink;
import cs451.perfect_links.PerfectLinkImpl;

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

    private static void sleepUntilNextNSecondMark(int n) {
        long currentTime = System.nanoTime();
        double interval = n * 1e9;
        double time = currentTime;
        time += interval;
        time = Math.ceil(time / interval) * interval;
        long endSleepTime = (long) time;
        try {
            System.out.println("sleeping");
            Thread.sleep((endSleepTime - currentTime) / 1_000_000);
            System.out.println("done");
        } catch (InterruptedException ignored) {
        }
    }

    private static void runPerfectLinksTest(Parser parser, int myPort, Map<Integer, FullAddress> addresses) {
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

        // find out who the receiver is
        boolean isReceiver = parser.myId() == receiverId;

        DatagramSocket socket;
        try {
            socket = new DatagramSocket(myPort);
            socket.setSoTimeout(10);
        } catch (SocketException e) {
            throw new Error(e);
        }


        System.out.println("Broadcasting and delivering messages...\n");

        long start = System.nanoTime();
        // for debug
        int expectedMessages = (parser.hosts().size() - 1) * numMessages;
        System.out.println("expecting " + expectedMessages + " messages");
        int[] totalMessages = {0};

        System.out.println("begin listening");
        PerfectLink perfectLink = new PerfectLinkImpl(parser.myId(), addresses, socket, delivered -> {
            eventHistory.logDelivery(delivered.getPlSourceId(), delivered.getPlSourceId());

//            System.out.println("deliver " + delivered.getText());

            // for debug
            totalMessages[0] += 1;
            int tm = totalMessages[0];
            if (tm % 100_000 == 0
                    || expectedMessages - tm < 1000 && tm % 100 == 0
                    || expectedMessages - tm < 100) {
                System.out.println("total: " + totalMessages[0]);
            }
            if (totalMessages[0] == expectedMessages) {
                long end = System.nanoTime();
                if (isReceiver) {
                    System.out.println("total number of messages received: " + totalMessages[0]);
                    System.out.println("time taken: " + (end - start) / 1_000_000 + "ms");
                    System.out.println("messages/s: " + ((long) (totalMessages[0] * 1e9) / (end - start)));
                }
            }
            if (totalMessages[0] > expectedMessages) {
                System.out.println("ok, this is not funny anymore");
                System.exit(1);
            }
        });

        // send
        if (!isReceiver) {
            for (int i = 0; i < numMessages; i++) {
                perfectLink.plSend("" + i, receiverId);
                eventHistory.logBroadcast(i);
            }
        }

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
    }

    private static void runFifoTest(Parser parser, int myPort, Map<Integer, FullAddress> addresses) {
        System.out.println("Doing some initialization\n");
        int numMessages;
        try {
            String config = Files.readString(Paths.get(parser.config())).stripTrailing();
            numMessages = Integer.parseInt(config);
        } catch (IOException e) {
            throw new Error(e);
        }

        DatagramSocket socket;
        try {
            socket = new DatagramSocket(myPort);
            socket.setSoTimeout(10);
        } catch (SocketException e) {
            throw new Error(e);
        }


        long[] start = {-1};

        int expectedMessages = numMessages * parser.hosts().size();
        System.out.println("Expecting " + expectedMessages + " messages");
        int[] totalMessages = new int[] {0};
        FIFO fifo = new FIFOImpl(parser.myId(), addresses, socket, delivered -> {
            eventHistory.logDelivery(delivered.getFifoSourceId(), delivered.getFifoMessageId());

            if (start[0] < 0) {
                start[0] = System.nanoTime();
            }

            totalMessages[0] += 1;
            if (totalMessages[0] >= expectedMessages) {
                long end = System.nanoTime();
                System.out.println("total number of messages received: " + totalMessages[0]);
                System.out.println("time taken: " + (end - start[0]) / 1_000_000 + "ms");
                System.out.println("messages/s: " + ((long) (totalMessages[0] * 1e9) / (end - start[0])));

                if (totalMessages[0] > expectedMessages) {
                    System.out.println("ERROR: DELIVERED MORE MESSAGES THAN EXPECTED");
                    System.exit(1);
                }
            } else {
                int tm = totalMessages[0];
                if (tm % 1000 == 0
                        || expectedMessages - tm < 1000 && tm % 100 == 0
                        || expectedMessages - tm < 100) {
                    System.out.println("total: " + totalMessages[0]);
                }
            }
        });

//        sleepUntilNextNSecondMark(5);

        for (int i = 0; i < numMessages; i++) {
            fifo.fifoBroadcast("" + i);
        }
    }

    public static void main(String[] args) {

        // TODO: remove
        if (args.length == 0) {
            int id = 2;
            args = new String[] {
                    "--id", "" + id,
                    "--hosts", "/Users/Gorilskij/Desktop/EPFL/Courses/DA/CS451-2021-project/template_java/hosts",
                    "--output", id + ".output",
//                    "/Users/Gorilskij/Desktop/EPFL/Courses/DA/CS451-2021-project/template_java/perfect-links.config"
                    "/Users/Gorilskij/Desktop/EPFL/Courses/DA/CS451-2021-project/template_java/fifo.config"
            };
        }

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

        Map<Integer, FullAddress> addresses = new HashMap<>();
        int myPort = -1;
        for (Host host : parser.hosts()) {
            InetAddress hostAddress;
            try {
                hostAddress = InetAddress.getByName(host.getIp());
            } catch (UnknownHostException e) {
                throw new IllegalStateException(e);
            }
            FullAddress fullAddress = new FullAddress(hostAddress, host.getPort());
            addresses.put(host.getId(), fullAddress);

            if (host.getId() == parser.myId()) {
                myPort = host.getPort();
            }
        }

        if (parser.config().contains("perfect-links")) {
            System.out.println("Running perfect links");
            runPerfectLinksTest(parser, myPort, addresses);
        } else {
            System.out.println("Running fifo");
            runFifoTest(parser, myPort, addresses);
        }
    }
}
