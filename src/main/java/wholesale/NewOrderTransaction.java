package wholesale;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static wholesale.Constants.KEYSPACE_NAME;

public class NewOrderTransaction {
    private CqlSession session;
    private PreparedStatement selectCustomerByDistrictStatement;
    private PreparedStatement selectStock;
    private PreparedStatement updateStock;
    private PreparedStatement selectItem;
    private PreparedStatement insertOrder;
    private PreparedStatement insertOrderDelivery;
    private PreparedStatement selectNextOid;

    private BatchStatementBuilder insertOrderBatch;

    private UserDefinedType orderLineType;

    private static final String SELECT_CUSTOMER =
            " SELECT * FROM " + Constants.TableName.CUSTOMER_DATA +
                    " WHERE " + Constants.Customer.WAREHOUSE_ID + " = ?  AND " +
                    Constants.Customer.DISTRICT_ID + " = ? AND " + Constants.Customer.ID + " = ?; ";

    private static final String SELECT_STOCK =
            " SELECT * FROM " + Constants.TableName.STOCK + " WHERE  S_W_ID = ? AND S_I_ID = ?; ";

    private static final String SELECT_NEXT_O_ID =
            " SELECT " + Constants.Order.D_NEXT_ORDER_ID  + " FROM " + Constants.TableName.ORDER +
                    " WHERE " + Constants.Order.WAREHOUSE_ID + " = ? AND " + Constants.Order.DISTRICT_ID + " = ? LIMIT 1";

    private static final String UPDATE_STOCK =
            " UPDATE stock SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ?" +
                    " WHERE S_W_ID = ? AND S_I_ID = ?; ";

    private static final String SELECT_ITEM = "SELECT * FROM " + Constants.TableName.ITEM +
            " WHERE I = 1 AND " + Constants.Item.ITEM_ID + " = ?; ";

    private static final String INSERT_ORDER = "INSERT INTO " + Constants.TableName.ORDER + " ( " +
            Constants.Order.WAREHOUSE_ID + ", " + Constants.Order.DISTRICT_ID + "," + Constants.Order.ORDER_ID + "," +
            Constants.Order.CUSTOMER_ID + "," + Constants.Order.D_NEXT_ORDER_ID + "," +
            Constants.Order.ORDERLINE_COUNT + "," + Constants.Order.ALL_COUNT + ", " + Constants.Order.ENTRY_DATE +
            ", " + Constants.Order.TOTAL_OL_QUANTITY + "," + Constants.Order.ITEM_IDS +
            ", " + Constants.Order.ORDER_LINES + ")" + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";

    private static final String INSERT_ORDER_DELIVERY = "INSERT INTO " + Constants.TableName.ORDER_CARRIER + " ( " +
            Constants.Order.WAREHOUSE_ID + ", " + Constants.Order.DISTRICT_ID + "," + Constants.Order.ORDER_ID + "," +
            Constants.Order.CUSTOMER_ID + "," + Constants.Order.CARRIER_ID + ", " + Constants.Order.TOTAL_AMOUNT + 
            ") VALUES (?, ?, ?, ?, ?, ?) ";

    public NewOrderTransaction(CqlSession session) {
        this.session = session;
        this.selectCustomerByDistrictStatement = this.session.prepare(SELECT_CUSTOMER);
        this.selectStock = this.session.prepare(SELECT_STOCK);
        this.updateStock = this.session.prepare(UPDATE_STOCK);
        this.selectItem = this.session.prepare(SELECT_ITEM);
        this.insertOrder = this.session.prepare(INSERT_ORDER);
        this.insertOrderDelivery = this.session.prepare(INSERT_ORDER_DELIVERY);
        this.selectNextOid = this.session.prepare(SELECT_NEXT_O_ID);
        orderLineType = session.getMetadata().getKeyspace(KEYSPACE_NAME)
                .get().getUserDefinedType(Constants.Udt.ORDER_LINE).get();
    }

    public boolean executeTransaction(int C_ID, int W_ID, int D_ID, List<InputOrderItem> orderItems) {
        insertOrderBatch = new BatchStatementBuilder(DefaultBatchType.LOGGED);
        StringBuilder printOutput = new StringBuilder();
        Row customer_data = getCustomerByDistrict(W_ID, D_ID, C_ID);
        List<UdtValue>orderlines = new ArrayList<>();
        ArrayList<Integer> itemIds = new ArrayList<>();
        double districtTax = customer_data.getBigDecimal(Constants.Customer.DISTRICT_TAX).doubleValue();
        double warehouseTax = customer_data.getBigDecimal(Constants.Customer.WAREHOUSE_TAX).doubleValue();

        ResultSet res = session.execute(this.selectNextOid.bind(W_ID, D_ID));
        Row row = res.one();
        if(row ==null){
            return false;
        }
        int nextOrderId = row.getInt(Constants.Order.D_NEXT_ORDER_ID);

        BigDecimal orderLineCount = new BigDecimal(orderItems.size());
        BigDecimal allLocal = new BigDecimal(1);
        Date currDate = new Date();
        for(InputOrderItem orderItem : orderItems){
            if(orderItem.supply_W_ID != W_ID){
                allLocal = new BigDecimal(0);
                break;
            }
        }
        double totalAmount = 0;
        UdtValue customer = customer_data.getUdtValue(Constants.Customer.NAME);
        printOutput.append("C_ID: " +C_ID + " C_W_ID: " + W_ID + " C_D_ID: " +D_ID +
                " C_LAST: " + customer.getString(Constants.CustomerName.LAST_NAME) + " C_CREDIT: " +
                customer_data.getString(Constants.Customer.CREDIT) + " C_DISCOUNT: " +
                customer_data.getBigDecimal(Constants.Customer.DISCOUNT));
        printOutput.append(" W_TAX: " + warehouseTax + " D_TAX: " + districtTax);
        printOutput.append(" O_ID: " + nextOrderId + " O_ENTRY_D: "+ currDate.toString());

        int i=1;
        int totalOrderLineQuantity = 0;
        for(InputOrderItem orderItem : orderItems) {
            Row stock = getStock(orderItem.supply_W_ID, orderItem.item_ID);
            int adjQuantity = stock.getInt("S_QUANTITY") - orderItem.quantity;
            while(adjQuantity < 10) {
                adjQuantity += 100;
            }
            updateStock(orderItem.supply_W_ID, orderItem.item_ID, adjQuantity,
                    stock.getBigDecimal("S_YTD").add(new BigDecimal(orderItem.quantity)),
                    stock.getInt("S_ORDER_CNT")+1,
                    orderItem.supply_W_ID != W_ID ? stock.getInt("S_REMOTE_CNT") + 1 : stock.getInt("S_REMOTE_CNT"));
            Row item = getItem(orderItem.item_ID);
            BigDecimal itemPrice = item.getBigDecimal(Constants.Item.PRICE);
            BigDecimal itemAmount = itemPrice.multiply(new BigDecimal(orderItem.quantity));
            totalAmount += itemAmount.doubleValue();
            totalOrderLineQuantity += orderItem.quantity;

            orderlines.add(getOrderLine(i, orderItem.item_ID, itemAmount,
                    orderItem.supply_W_ID, new BigDecimal(orderItem.quantity)));
            itemIds.add(orderItem.item_ID);
            i++;
            printOutput.append(", Item Number: "+ orderItem.item_ID + " I_NAME: "+ item.getString(Constants.Item.NAME) +
                    " SUPPLY_W_ID: "+ orderItem.supply_W_ID + " Quantity: "+ orderItem.quantity + "OL_AMOUNT: "+ itemAmount +
                    " S_QUANTITY: " + adjQuantity);

        }

        createNewOrder(W_ID, D_ID, nextOrderId, C_ID, orderLineCount, allLocal, currDate,
                totalOrderLineQuantity, itemIds, orderlines);

        insertOrderBatch.addStatement(insertOrderDelivery.bind(W_ID, D_ID, nextOrderId, C_ID, -1, new BigDecimal(totalAmount)));

        totalAmount = totalAmount * (1 + warehouseTax + districtTax) *
                (1 - customer_data.getBigDecimal(Constants.Customer.DISCOUNT).doubleValue());
        printOutput.append(" NUM_ITEMS: "+ orderItems.size() + " TOTAL_AMOUNT: "+ totalAmount);

        ResultSet result = session.execute(insertOrderBatch.build());
        if (result.wasApplied()) {
                System.out.println(printOutput.toString());
        }
        return result.wasApplied();
    }
    public Row getCustomerByDistrict(int W_ID, int D_ID, int C_ID) {
        ResultSet customerByDistrict = this.session.execute(selectCustomerByDistrictStatement.bind(W_ID, D_ID, C_ID).setConsistencyLevel(ConsistencyLevel.ONE));
        return customerByDistrict.one();
    }
    public Row getStock(int supply_W_ID, int item_ID) {
        ResultSet stock = session.execute(selectStock.bind(supply_W_ID, item_ID).setConsistencyLevel(ConsistencyLevel.ONE));
        return stock.one();
    }
    public void updateStock(int supply_W_ID, int item_ID, int adjQuantity,
                            BigDecimal ytd, int orderCount, int remoteCount) {
        insertOrderBatch.addStatement(updateStock.bind(adjQuantity, ytd, orderCount, remoteCount, supply_W_ID, item_ID).setConsistencyLevel(ConsistencyLevel.ALL));
    }
    public Row getItem(int item_ID) {
        ResultSet item = session.execute(selectItem.bind(item_ID).setConsistencyLevel(ConsistencyLevel.ONE));
        return item.one();
    }
    public void createNewOrder(int W_ID, int D_ID, int nextOrderId, int C_ID, BigDecimal orderLineCount,
                               BigDecimal allLocal, Date currDate, int totalOrderLineQuantity,
                               List<Integer>itemIds, List<UdtValue> orderlines) {
        insertOrderBatch.addStatement(insertOrder.bind(W_ID, D_ID, nextOrderId, C_ID, nextOrderId+1,
                orderLineCount, allLocal, currDate.toInstant(), totalOrderLineQuantity, itemIds, orderlines).setConsistencyLevel(ConsistencyLevel.ALL));
    }
    public UdtValue getOrderLine(int OL_NUMBER, int OL_I_ID, BigDecimal OL_AMOUNT,
                                 int OL_SUPPLY_W_ID, BigDecimal OL_QUANTITY) {
        return orderLineType.newValue()
                .setInt(Constants.OrderLine.NUMBER, OL_NUMBER)
                .setInt(Constants.OrderLine.ITEM_ID, OL_I_ID)
                .setBigDecimal(Constants.OrderLine.AMOUNT, OL_AMOUNT)
                .setInt(Constants.OrderLine.SUPPLY_W_ID, OL_SUPPLY_W_ID)
                .setBigDecimal(Constants.OrderLine.QUANTITY, OL_QUANTITY);
    }
}
