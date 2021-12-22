package wholesale;

import java.util.AbstractMap;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import wholesale.Constants.Customer;
import wholesale.Constants.Order;
import wholesale.Constants.TableName;

public class DeliveryTransactionHandler {
    
    private CqlSession session;

    private PreparedStatement selectSmallestUndeliveredOrdersPrepared;
    private PreparedStatement selectCustomerOrderStatsPrepared;
    private PreparedStatement updateOrderDeliveryStatusPrepared;
    private PreparedStatement updateCustomerPaymentPrepared;

    public DeliveryTransactionHandler(CqlSession session) {
        this.session = session;
        createPreparedStatements();
    }

    public boolean execute(int warehouseId, int carrierId) {
        Date deliveryDate = new Date();
        ResultSet result;

        HashMap<Map.Entry<Integer, Integer>, Map.Entry<Row, Row>> customerOrderBalanceMap = new HashMap<>();

        result = session.execute(selectSmallestUndeliveredOrdersPrepared.boundStatementBuilder()
                .setInt("w_id", warehouseId)
                .setInt("carrier_id", -1)
                .build());

        List<Row> orderList = result.all();
        for (Row order: orderList) {
            int customerId = order.getInt(Order.CUSTOMER_ID);
            int districtId = order.getInt(Order.DISTRICT_ID);

            ResultSet customerResult = session.execute(selectCustomerOrderStatsPrepared.boundStatementBuilder()
                .setInt("w_id", warehouseId)
                .setInt("d_id", districtId)
                .setInt("c_id", customerId)
                .build()
            );

            Row customer = customerResult.one();
            customerOrderBalanceMap.put(Pair.of(districtId, customerId), Pair.of(order, customer));
        }

        BatchStatementBuilder districtLevelTransactionBatch = new BatchStatementBuilder(DefaultBatchType.LOGGED);

        for (Map.Entry<Integer, Integer> id: customerOrderBalanceMap.keySet()) {
            Row order = customerOrderBalanceMap.get(id).getKey();
            Row customerBalance = customerOrderBalanceMap.get(id).getValue();

            districtLevelTransactionBatch.addStatement(updateOrderDeliveryStatusPrepared.boundStatementBuilder()
                .setInt("carrier_id", carrierId)
                .setInstant("deli_date", deliveryDate.toInstant())
                .setInt("w_id", warehouseId)
                .setInt("d_id", order.getInt(Order.DISTRICT_ID))
                .setInt("c_id", order.getInt(Order.CUSTOMER_ID))
                .setInt("o_id", order.getInt(Order.ORDER_ID))
                .build());
            districtLevelTransactionBatch.addStatement(updateCustomerPaymentPrepared.boundStatementBuilder()
                .setBigDecimal("balance", customerBalance.getBigDecimal(Customer.BALANCE).add(order.getBigDecimal(Order.TOTAL_AMOUNT)))
                .setInt("deli_cnt", customerBalance.getInt(Customer.DELIVERY_CNT) + 1)
                .setInt("w_id", warehouseId)
                .setInt("d_id", order.getInt(Order.DISTRICT_ID))
                .setInt("c_id", order.getInt(Order.CUSTOMER_ID))
                .build()
            );
        }
        
        result = session.execute(districtLevelTransactionBatch.build());
        return result.wasApplied();
    }

    static class Pair {
        public static <T, U> Map.Entry<T, U> of(T first, U second) {
            return new AbstractMap.SimpleEntry<>(first, second);
        }
    }
 
    private void createPreparedStatements() {
        this.selectSmallestUndeliveredOrdersPrepared = session.prepare(QueryBuilder.selectFrom(TableName.ORDER_CARRIER)
            .all()
            .whereColumn(Order.WAREHOUSE_ID).isEqualTo(QueryBuilder.bindMarker("w_id"))
            .whereColumn(Order.CARRIER_ID).isEqualTo(QueryBuilder.bindMarker("carrier_id"))
            .groupBy(Order.DISTRICT_ID)
            .allowFiltering()
            .build()
        );
    
        this.selectCustomerOrderStatsPrepared = session.prepare(QueryBuilder.selectFrom(TableName.CUSTOMER_ORDER_STATS)
            .columns(Customer.BALANCE, Customer.DELIVERY_CNT)
            .whereColumn(Customer.WAREHOUSE_ID).isEqualTo(QueryBuilder.bindMarker("w_id"))
            .whereColumn(Customer.DISTRICT_ID).isEqualTo(QueryBuilder.bindMarker("d_id"))
            .whereColumn(Customer.ID).isEqualTo(QueryBuilder.bindMarker("c_id"))
            .build()
        );

        this.updateOrderDeliveryStatusPrepared = session.prepare(QueryBuilder.update(TableName.ORDER_CARRIER)
            .setColumn(Order.CARRIER_ID, QueryBuilder.bindMarker("carrier_id"))
            .setColumn(Order.DELIVERY_DATE, QueryBuilder.bindMarker("deli_date"))
            .whereColumn(Order.WAREHOUSE_ID).isEqualTo(QueryBuilder.bindMarker("w_id"))
            .whereColumn(Order.DISTRICT_ID).isEqualTo(QueryBuilder.bindMarker("d_id"))
            .whereColumn(Order.CUSTOMER_ID).isEqualTo(QueryBuilder.bindMarker("c_id"))
            .whereColumn(Order.ORDER_ID).isEqualTo(QueryBuilder.bindMarker("o_id"))
            .build()
        );

        this.updateCustomerPaymentPrepared = session.prepare(QueryBuilder.update(TableName.CUSTOMER_ORDER_STATS)
            .setColumn(Customer.BALANCE, QueryBuilder.bindMarker("balance"))
            .setColumn(Customer.DELIVERY_CNT, QueryBuilder.bindMarker("deli_cnt"))
            .whereColumn(Customer.WAREHOUSE_ID).isEqualTo(QueryBuilder.bindMarker("w_id"))
            .whereColumn(Customer.DISTRICT_ID).isEqualTo(QueryBuilder.bindMarker("d_id"))
            .whereColumn(Customer.ID).isEqualTo(QueryBuilder.bindMarker("c_id"))
            .build()
        );
    }
}
