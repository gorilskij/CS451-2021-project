package cs451;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class EventHistory {
    private final StringBuilder builder = new StringBuilder();

    public void logBroadcast(int seqNr) {
        builder.append("b ");
        builder.append(seqNr);
        builder.append("\n");
    }

    public void logDelivery(int senderId, int seqNr) {
        builder.append("d ");
        builder.append(senderId);
        builder.append(" ");
        builder.append(seqNr);
        builder.append("\n");
    }

    public void writeToFile(String filePath) {
        String contents = builder.toString();

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
            writer.write(contents);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
