package wholesale;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;

public final class Util {

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public static final boolean loggingEnabled = true;

    public static interface FileLineReader {

        void read(String line);
    }

    public static void readFile(String filePath, FileLineReader reader) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            for (String line; (line = br.readLine()) != null;) {
                // process the line.
                reader.read(line);
            }
        } catch (IOException e) {
            System.err.println(e.toString());
        }
    }

    public static void printTransactionOutput(Row row, String[] columns, HashMap<String, String> others) {
        StringBuilder builder = new StringBuilder();
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if (i > 0) {
                    builder.append(",");
                }
                builder.append(column).append(":");
                if (others.containsKey(column)) {
                    builder.append(others.get(column));
                } else {    
                    TypeCodec<Object> codec = row.codecRegistry().codecFor(row.getType(column));
                    Object value = codec.decode(row.getBytesUnsafe(column), row.protocolVersion());
                    builder.append(codec.format(value));
                }
            }
            System.out.println(builder.toString());
    }

    public static void debugLog(String className, String message) {
        if (loggingEnabled) {
            System.out.printf("[DEBUG] t=%d | %s: %s%n", System.currentTimeMillis(), className, message);
        }
    }

    private Util() {

    }
}
