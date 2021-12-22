package wholesale;

import com.datastax.oss.driver.api.core.CqlSession;

public class Setup {
    
    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder().build()) {
            DatabaseManager.setup(session, Constants.KEYSPACE_NAME);
        }
    }
}
