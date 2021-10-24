package cs451;

import cs451.perfectLinks.PerfectLink;

import java.io.IOException;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;

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

    public static void main(String[] args) throws InterruptedException {

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


//        // TODO: remove
//        try {
//            Thread.sleep(10000);
//        } catch (InterruptedException ignored) {
//        }


        System.out.println("Broadcasting and delivering messages...\n");

        boolean isReceiver = parser.myId() == receiverId;


        long start = System.nanoTime();
        int expectedMsgs = (parser.hosts().size() - 1) * numMessages;
        int[] totalMsgs = {0};

        PerfectLink perfectLink = new PerfectLink(parser.myId(), socket, delivered -> {
            totalMsgs[0] += 1;

//            System.out.println("DELIVER \"" + delivered.text + "\"");
//            System.out.println("  total: " + totalMsgs[0]);

            eventHistory.logDelivery(delivered.sourceId, delivered.messageId);

            if (totalMsgs[0] == expectedMsgs) {
                long end = System.nanoTime();
                if (isReceiver) {
                    System.out.println("total number of messages received: " + totalMsgs[0]);
                    System.out.println("time taken: " + (end - start) / 1_000_000 + "ms");
                    System.out.println("messages/s: " + ((long) (totalMsgs[0] * 1e9) / (end - start)));
                }
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
}
