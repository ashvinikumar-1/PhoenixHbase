import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by ashvinikumar on 31/7/17.
 */
public class CreateCsv {
    public static void main(String args[]) throws IOException {
        FileWriter writer = new FileWriter("/home/ashvinikumar/keyValuePair.csv");
        int i=20;
        for (i=20; i < 1000000; i++) {
            writer.append(Integer.toString(i));
            writer.append(',');
            writer.append("AK"+Integer.toString(i-19));
            writer.append(',');
            writer.append("Bangalore");
            writer.append('\n');


        }
        writer.flush();
        writer.close();
    }
}
