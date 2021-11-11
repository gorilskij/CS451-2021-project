package cs451;

import cs451.base.FullAddress;
import cs451.base.Pair;
import cs451.perfect_links.PerfectLink;
import cs451.uniform_reliable_broadcast.URB;

import java.io.IOException;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    private static void runPerfectLinksTest(Parser parser) {
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

            if (receiverPort > -1 && myPort > -1) {
                break;
            }
        }
        boolean isReceiver = parser.myId() == receiverId;


        FullAddress receiverFullAddress = new FullAddress(receiverAddress, receiverPort);
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

        PerfectLink perfectLink = new PerfectLink(parser.myId(), socket, delivered -> {
            eventHistory.logDelivery(delivered.sourceId, delivered.messageId);

            // for debug
            totalMessages[0] += 1;
            System.out.println("total: " + totalMessages[0]);
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
                perfectLink.send("message " + i + " from process " + parser.myId(), receiverFullAddress);
                eventHistory.logBroadcast(i);
            }
        }

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
    }

    private static void runFifoTest(Parser parser) {
        System.out.println("Doing some initialization\n");
        int numMessages;
        try {
            String config = Files.readString(Paths.get(parser.config())).stripTrailing();
            numMessages = Integer.parseInt(config);
        } catch (IOException e) {
            throw new Error(e);
        }

        List<Pair<Integer, FullAddress>> allProcesses = new ArrayList<>();
        int myPort = -1;
        for (Host host : parser.hosts()) {
            InetAddress hostAddress;
            try {
                hostAddress = InetAddress.getByName(host.getIp());
            } catch (UnknownHostException e) {
                throw new IllegalStateException(e);
            }
            FullAddress fullAddress = new FullAddress(hostAddress, host.getPort());
            allProcesses.add(new Pair<>(host.getId(), fullAddress));

            if (host.getId() == parser.myId()) {
                myPort = host.getPort();
            }
        }

        DatagramSocket socket;
        try {
            socket = new DatagramSocket(myPort);
            socket.setSoTimeout(10);
        } catch (SocketException e) {
            throw new Error(e);
        }

        int expectedMessages = numMessages * parser.hosts().size();
        System.out.println("Expecting " + expectedMessages + " messages");
        int[] totalMessages = new int[] {0};
        URB urb = new URB(parser.myId(), allProcesses, socket, message -> {
            totalMessages[0] += 1;
            if (totalMessages[0] >= expectedMessages) {
//                System.out.println("DELIVER: \"" + message + "\"");

                if (totalMessages[0] > expectedMessages) {
                    System.out.println("ERROR: DELIVERED MORE MESSAGES THAN EXPECTED");
                    System.exit(1);
                }
            }
            System.out.println("    total: " + totalMessages[0]);
        });

        for (int i = 0; i < numMessages; i++) {
            String message = "message " + i + " from process " + parser.myId();
//            System.out.println("BROADCAST: \"" + message + "\"");
            urb.broadcast(message);
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
                    "/Users/Gorilskij/Desktop/EPFL/Courses/DA/CS451-2021-project/template_java/perfect-links.config"
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

//        runPerfectLinksTest(parser);
        runFifoTest(parser);
    }
}
