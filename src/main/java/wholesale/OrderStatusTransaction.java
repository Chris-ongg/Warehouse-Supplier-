package wholesale;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public class OrderStatusTransaction {
    private CqlSession session;
    
    private static final String SELECT_CUSTOMER =
            " SELECT " + Constants.Customer.NAME + " , " + Constants.Customer.BALANCE +
            " FROM " + Constants.TableName.CUSTOMER_ORDER_STATS + " WHERE " + Constants.Customer.WAREHOUSE_ID +
            " = ? AND " + Constants.Customer.DISTRICT_ID + " = ? AND " +
            Constants.Customer.ID + " = ?";
    private static final String SELECT_ORDER = "SELECT " + Constants.Order.ORDER_ID + "," + Constants.Order.ENTRY_DATE + "," +
            Constants.Order.ORDER_LINES +
            " FROM " + Constants.TableName.ORDER + " WHERE " +
            Constants.Order.WAREHOUSE_ID + " = ? AND " +
            Constants.Order.DISTRICT_ID + " = ? AND " + Constants.Order.CUSTOMER_ID + " = ? ORDER BY " + 
            Constants.Order.ORDER_ID + " DESC LIMIT 1";
    private static final String SELECT_ORDER_CARRIER =
            " SELECT " + Constants.Order.CARRIER_ID + "," + Constants.Order.DELIVERY_DATE +
            " FROM " + Constants.TableName.ORDER_CARRIER + " WHERE " + Constants.Order.WAREHOUSE_ID +
            " = ? AND " + Constants.Order.DISTRICT_ID + " = ? AND " + Constants.Order.ORDER_ID + " = ? AND " +
            Constants.Order.CUSTOMER_ID + " = ?";        

    private PreparedStatement selectCustomer;
    private PreparedStatement selectOrder;
    private PreparedStatement selectOrderCarrier;

    public OrderStatusTransaction(CqlSession session) {
        this.session = session;
        this.selectCustomer = session.prepare(SELECT_CUSTOMER);
        this.selectOrder = session.prepare(SELECT_ORDER);
        this.selectOrderCarrier = session.prepare(SELECT_ORDER_CARRIER);

    }
    public void executeTransaction (int W_ID, int D_ID, int C_ID) {

        Row customer = session.execute(selectCustomer.bind(W_ID, D_ID, C_ID).setConsistencyLevel(ConsistencyLevel.ONE)).one();
        if(customer == null)return;
        UdtValue customerName = customer.getUdtValue(Constants.Customer.NAME);
        String firstName = customerName.getString(Constants.CustomerName.FIRST_NAME);
        String middleName = customerName.getString(Constants.CustomerName.MIDDLE_NAME);
        String lastName = customerName.getString(Constants.CustomerName.LAST_NAME);
        BigDecimal customerBalance = customer.getBigDecimal(Constants.Customer.BALANCE);

        System.out.println("First Name: " + firstName + " Middle Name: "+ middleName + " Last Name: "+lastName);
        System.out.println("Customer Balance: "+ customerBalance);

        Row order = session.execute(selectOrder.bind(W_ID, D_ID, C_ID).setConsistencyLevel(ConsistencyLevel.ONE)).one();
        int orderNumber = order.getInt(Constants.Order.ORDER_ID);
        Instant entryDate = order.getInstant(Constants.Order.ENTRY_DATE);

        Row orderCarrier = session.execute(selectOrderCarrier.bind(W_ID, D_ID, orderNumber, C_ID)).one();
        int carrierId = orderCarrier.getInt(Constants.Order.CARRIER_ID);

        System.out.println("Order Number: "+ orderNumber + " Entry Date" + entryDate.toString() + " Carrier ID: "+ carrierId);

        List<UdtValue> orderlines = order.getList(Constants.Order.ORDER_LINES,UdtValue.class);
        Instant deliveryTime = orderCarrier.getInstant(Constants.Order.DELIVERY_DATE);

        System.out.println("Items details: ");
        for (UdtValue orderLine : orderlines) {
            int itemId = orderLine.getInt(Constants.OrderLine.ITEM_ID);
            int supplyWarehouseId = orderLine.getInt(Constants.OrderLine.SUPPLY_W_ID);
            BigDecimal quantity = orderLine.getBigDecimal(Constants.OrderLine.QUANTITY);
            BigDecimal amount = orderLine.getBigDecimal(Constants.OrderLine.AMOUNT);
            System.out.println("Item ID: "+ itemId + " Supply Warehouse ID: " + supplyWarehouseId +
                    " Quantity: " + quantity + " Amount: " + amount + " Delivery Time: " + deliveryTime);
        }


    }
}
