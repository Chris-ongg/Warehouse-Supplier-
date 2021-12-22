package wholesale;

import java.math.BigDecimal;
import java.util.HashMap;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import wholesale.Constants.Customer;
import wholesale.Constants.TableName;

public class PaymentTransactionHandler {

    private CqlSession session;

    private static final String[] OUTPUT_COLUMNS = {"C_W_ID", "C_D_ID", "C_ID", "C_NAME", "C_ADDRESS", "C_PHONE", 
        "C_SINCE", "C_CREDIT", "C_CREDIT_LIM", "C_DISCOUNT", "C_BALANCE", "W_ADDRESS", "D_ADDRESS", "C_PAYMENT_CNT"};

    private PreparedStatement selectCustomerPrepared;
    private PreparedStatement selectCustomerOrderStatsPrepared;
    private PreparedStatement updateCustomerPaymentPrepared;

    public PaymentTransactionHandler(CqlSession session) {
        this.session = session;
        createPreparedStatements();
    }

    public boolean execute(int warehouseId, int districtId, int customerId, BigDecimal payment) {
        
        ResultSet selectCustomerDataResult = session.execute(selectCustomerPrepared.boundStatementBuilder()
            .setInt("w_id", warehouseId)
            .setInt("d_id", districtId)
            .setInt("c_id", customerId)
            .setConsistencyLevel(ConsistencyLevel.ONE)
            .build()
        );

        ResultSet selectCustomerOrderStatsResult = session.execute(selectCustomerOrderStatsPrepared.boundStatementBuilder()
            .setInt("w_id", warehouseId)
            .setInt("d_id", districtId)
            .setInt("c_id", customerId)
            .build()
        );

        Row customerInfo = selectCustomerDataResult.one();
        Row customerOrderStats = selectCustomerOrderStatsResult.one();

        BigDecimal currentDistrictYtd = customerOrderStats.getBigDecimal(String.format("D_%d_YTD", districtId));
        int currentPaymentCnt = customerOrderStats.getInt(Customer.PAYMENT_CNT);
        BigDecimal currentBalance = customerOrderStats.getBigDecimal(Customer.BALANCE);
        Float currentCustomerYtd = customerOrderStats.getFloat(Customer.YTD_PAYMENT);

        BigDecimal newDistrictYtd = currentDistrictYtd.add(payment);
        int newPaymentCnt = currentPaymentCnt + 1;
        BigDecimal newBalance = currentBalance.subtract(payment);
        Float newCustomerYtd = Float.sum(currentCustomerYtd, payment.floatValue());


        BoundStatement updateCustomerPaymentStmt = updateCustomerPaymentPrepared.boundStatementBuilder()
            .setBigDecimal("balance", newBalance)
            .setFloat("c_ytd", newCustomerYtd)
            .setInt("payment_cnt", newPaymentCnt)
            .setInt("w_id", warehouseId)
            .setInt("d_id", districtId)
            .setInt("c_id", customerId)
            .setInt("old_payment_cnt", currentPaymentCnt)
            .build();
        
        
        SimpleStatement updateDistrictYtdStmt = SimpleStatement.newInstance(
            String.format("UPDATE %s SET D_%d_YTD = %s WHERE C_W_ID = %d", TableName.CUSTOMER_ORDER_STATS, districtId, newDistrictYtd, warehouseId));
        
        ResultSet updateResult = session.execute(BatchStatement.builder(DefaultBatchType.LOGGED)
            .addStatement(updateCustomerPaymentStmt)
            .addStatement(updateDistrictYtdStmt)
            .build()
        );

        boolean isSuccess = updateResult.wasApplied();
        if (isSuccess) {
            HashMap<String, String> otherColumns = new HashMap<>();
            otherColumns.put(Customer.WAREHOUSE_ID, String.valueOf(warehouseId));
            otherColumns.put(Customer.DISTRICT_ID, String.valueOf(districtId));
            otherColumns.put(Customer.ID, String.valueOf(customerId));
            otherColumns.put(Customer.PAYMENT_CNT, String.valueOf(newPaymentCnt));
            otherColumns.put(Customer.BALANCE, newBalance.toPlainString());
            Util.printTransactionOutput(customerInfo, OUTPUT_COLUMNS, otherColumns);
        }
        
        return isSuccess;
    }

    private void createPreparedStatements() {
        this.selectCustomerPrepared = session.prepare(
            QueryBuilder.selectFrom(TableName.CUSTOMER_DATA)
                .columns(Customer.WAREHOUSE_ADDRESS, Customer.DISTRICT_ADDRESS, Customer.NAME, 
                        Customer.ADDRESS, Customer.PHONE, Customer.SINCE, Customer.CREDIT, 
                        Customer.CREDIT_LIM, Customer.DISCOUNT)
                .whereColumn(Customer.WAREHOUSE_ID)
                .isEqualTo(QueryBuilder.bindMarker("w_id"))
                .whereColumn(Customer.DISTRICT_ID)
                .isEqualTo(QueryBuilder.bindMarker("d_id"))
                .whereColumn(Customer.ID)
                .isEqualTo(QueryBuilder.bindMarker("c_id"))
                .build()
        );

        this.selectCustomerOrderStatsPrepared = session.prepare(
            QueryBuilder.selectFrom(TableName.CUSTOMER_ORDER_STATS)
                .all()
                .whereColumn(Customer.WAREHOUSE_ID)
                .isEqualTo(QueryBuilder.bindMarker("w_id"))
                .whereColumn(Customer.DISTRICT_ID)
                .isEqualTo(QueryBuilder.bindMarker("d_id"))
                .whereColumn(Customer.ID)
                .isEqualTo(QueryBuilder.bindMarker("c_id"))
                .build()
        );
        this.updateCustomerPaymentPrepared = session.prepare(
            QueryBuilder.update(TableName.CUSTOMER_ORDER_STATS)
                .setColumn(Customer.BALANCE, QueryBuilder.bindMarker("balance"))
                .setColumn(Customer.YTD_PAYMENT, QueryBuilder.bindMarker("c_ytd"))
                .setColumn(Customer.PAYMENT_CNT, QueryBuilder.bindMarker("payment_cnt"))
                .whereColumn(Customer.WAREHOUSE_ID)
                .isEqualTo(QueryBuilder.bindMarker("w_id"))
                .whereColumn(Customer.DISTRICT_ID)
                .isEqualTo(QueryBuilder.bindMarker("d_id"))
                .whereColumn(Customer.ID)
                .isEqualTo(QueryBuilder.bindMarker("c_id"))
                .ifColumn(Customer.PAYMENT_CNT).isEqualTo(QueryBuilder.bindMarker("old_payment_cnt"))
                .build()
        );
    }
}
