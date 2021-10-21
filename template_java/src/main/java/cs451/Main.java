package cs451;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Main {

    private static void handleSignal() {
        //immediately stop network packet processing
        System.out.println("Immediately stopping network packet processing.");

        //write/flush output file if necessary
        System.out.println("Writing output.");
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

        System.out.println("MY PORT: " + myPort);

        DatagramSocket socket;
        try {
            socket = new DatagramSocket(myPort);
            socket.setSoTimeout(10);
        } catch (SocketException e) {
            throw new Error(e);
        }

        ReceiverProcess receiverProcess = new ReceiverProcess(parser.myId(), socket);

        System.out.println("Broadcasting and delivering messages...\n");
        if (parser.myId() != receiverId) {
            SenderProcess senderProcess = new SenderProcess(parser.myId(), socket, numMessages, receiverFullAddress);
            senderProcess.start();
            senderProcess.join();
        }
        receiverProcess.start();
        receiverProcess.join(); // never terminates

        // After a process finishes broadcasting,
        // it waits forever for the delivery of messages.
    }
}
