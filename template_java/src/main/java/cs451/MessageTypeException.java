package cs451;

public class MessageTypeException extends Exception {
    public MessageTypeException(byte expected, byte got) {
        super("Invalid messageType, expected " + expected + " but got " + got);
    }
}
