package wholesale;

import com.datastax.oss.driver.api.core.CqlSession;

import java.util.concurrent.Callable;

public class ClientThread implements Callable<Measurement> {
    private int index;
    private String filePath;
    private CqlSession session;
    private TransactionHandler transactionHandler;
    public ClientThread(int index, CqlSession session){
        this.index = index;
        this.session = session;
        this.filePath = String.format("%s/%d.txt", Constants.TRANSACTION_DIR, index);
    }
    @Override()
    public Measurement call() {
        this.transactionHandler = new TransactionHandler(session, index, filePath);
        Measurement m = this.transactionHandler.processTransactions();
        m.index = this.index;
        return m;
    }
}
