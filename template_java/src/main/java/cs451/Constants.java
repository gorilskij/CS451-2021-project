package cs451;

public class Constants {
    public static final int ARG_LIMIT_CONFIG = 7;

    // indexes for id
    public static final int ID_KEY = 0;
    public static final int ID_VALUE = 1;

    // indexes for hosts
    public static final int HOSTS_KEY = 2;
    public static final int HOSTS_VALUE = 3;

    // indexes for output
    public static final int OUTPUT_KEY = 4;
    public static final int OUTPUT_VALUE = 5;

    // indexes for config
    public static final int CONFIG_VALUE = 6;

    // maximum size of a packet in bytes including metadata
    public static final int MAX_PACKET_SIZE = 65_000;


    public static final int SOCKET_SO_TIMEOUT = 10;

    public static final int SCHEDULER_THREADS = 5;

    // perfect links parameters
    public static final int PL_SENDING_INTERVAL = 10; // ms
    public static final int PL_SENDING_BATCH_SIZE = 100;
    public static final int PL_MIN_ACKS_PER_PACKET = 100;

    // uniform reliable broadcast parameters
    public static final int URB_SENDING_BATCH_SIZE = 100; // also applies to FIFO
}
