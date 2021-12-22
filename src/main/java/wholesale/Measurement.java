package wholesale;

import java.util.List;

public class Measurement {
    int index;
    long transactionCount;
    List<Long> transactionLatency;
    public Measurement(int index, long transactionCount, List<Long> transactionLatency) {
        this.index = index;
        this.transactionCount = transactionCount;
        this.transactionLatency = transactionLatency;
    }
}
