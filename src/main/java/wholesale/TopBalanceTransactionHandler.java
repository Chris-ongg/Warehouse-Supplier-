package wholesale;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import wholesale.Constants.Customer;
import wholesale.Constants.TableName;

public class TopBalanceTransactionHandler {

    private static final int NUM_CUSTOMERS = 10;
    private static final String[] COLUMNS = {Customer.NAME, Customer.BALANCE, Customer.WAREHOUSE_NAME, Customer.DISTRICT_NAME};
    
    private CqlSession session;

    private PreparedStatement getTopCustomerBalancePrepared;
    private PreparedStatement getCustomerInfoPrepared;

    public TopBalanceTransactionHandler(CqlSession session) {
        this.session = session;
        createPreparedStatements();
    }

    public void execute() {
        ResultSet topBalanceCustomers = session.execute(getTopCustomerBalancePrepared.bind());

        List<Row> topBalanceCustomerRows = topBalanceCustomers.all();

        Collections.sort(topBalanceCustomerRows, new Comparator<Row>() {
            @Override
            public int compare(Row cb1, Row cb2) {
                return cb2.getBigDecimal(Customer.BALANCE).compareTo(cb1.getBigDecimal(Customer.BALANCE));
            }
        });

        for (int i=0; i < NUM_CUSTOMERS; i++) {
            Row row = topBalanceCustomerRows.get(i);
            ResultSet districtInfoResult = session.execute(getCustomerInfoPrepared.boundStatementBuilder(
                row.getInt(Customer.WAREHOUSE_ID), row.getInt(Customer.DISTRICT_ID), row.getInt(Customer.ID))
                .build());
            HashMap<String, String> others = new HashMap<>();
            others.put(Customer.BALANCE, row.getBigDecimal(Customer.BALANCE).toPlainString());
            Util.printTransactionOutput(districtInfoResult.one(), COLUMNS, others);
        }
    }

    private void createPreparedStatements() {
        getCustomerInfoPrepared = session.prepare(QueryBuilder.selectFrom(TableName.CUSTOMER_DATA)
            .columns(Customer.WAREHOUSE_NAME, Customer.DISTRICT_NAME, Customer.NAME)
            .whereColumn(Customer.WAREHOUSE_ID).isEqualTo(QueryBuilder.bindMarker())
            .whereColumn(Customer.DISTRICT_ID).isEqualTo(QueryBuilder.bindMarker())
            .whereColumn(Customer.ID).isEqualTo(QueryBuilder.bindMarker())
            .build()
            .setConsistencyLevel(ConsistencyLevel.ONE)
        );
        getTopCustomerBalancePrepared = session.prepare(QueryBuilder.selectFrom(TableName.CUSTOMER_BALANCE)
            .columns(Customer.WAREHOUSE_ID, Customer.DISTRICT_ID, Customer.ID, Customer.BALANCE)
            .perPartitionLimit(NUM_CUSTOMERS)
            .build().setConsistencyLevel(ConsistencyLevel.ONE)
        );
    }
}
