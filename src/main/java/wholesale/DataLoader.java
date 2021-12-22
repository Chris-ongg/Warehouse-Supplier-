package wholesale;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import wholesale.Constants.Address;
import wholesale.Constants.TableName;
import wholesale.Constants.Udt;

import java.math.BigDecimal;
import java.text.ParseException;

import static wholesale.Util.DATE_FORMAT;

public class DataLoader {

    private static DataLoader INSTANCE;

    private CqlSession session;
    private String keyspace;

    private PreparedStatement insertCustomerStmt;
    private PreparedStatement insertDistrictStmt;
    private PreparedStatement insertWarehouseStmt;
    private PreparedStatement insertCustomerOrderStatsStmt;
    private PreparedStatement insertItemsStmt;
  /*  private PreparedStatement insertOrderStmt;
    private PreparedStatement insertStocksStmt;*/

    private UserDefinedType addressUdt;
    private UserDefinedType customerNameUdt;
    private UserDefinedType orderLineUdt;

    private BatchStatementBuilder insertCustomerStatementBatch = new BatchStatementBuilder(DefaultBatchType.UNLOGGED);
    private BatchStatementBuilder insertCustomerOrderStatsBatch = new BatchStatementBuilder(DefaultBatchType.UNLOGGED);
    private int insertCustomerLineCount = 0;

    public static DataLoader getInstance(CqlSession session, String keyspace) {
        if (INSTANCE == null) {
            INSTANCE = new DataLoader(session, keyspace);
        }
        return INSTANCE;
    }
    
    public void insertCustomer(String line) {
        String[] values = line.split(",");
        
        try {
            insertCustomerStatementBatch.addStatement(insertCustomerStmt.boundStatementBuilder()
                .setInt("w_id", Integer.parseInt(values[0]))
                .setInt("d_id", Integer.parseInt(values[1]))
                .setInt("c_id", Integer.parseInt(values[2]))
                .setUdtValue("name", getCustomerName(values[3], values[4], values[5]))
                .setUdtValue("address", getAddress(values[6], values[7], values[8], values[9], values[10]))
                .setString("phone", values[11])
                .setInstant("since", DATE_FORMAT.parse(values[12]).toInstant())
                .setString("credit", values[13])
                .setBigDecimal("lim", new BigDecimal(values[14]))
                .setBigDecimal("dis", new BigDecimal(values[15]))
                .setString("data", values[20]).build());

            insertCustomerOrderStatsBatch.addStatement(insertCustomerOrderStatsStmt.boundStatementBuilder()
                .setInt("w_id", Integer.parseInt(values[0]))
                .setInt("d_id", Integer.parseInt(values[1]))
                .setInt("c_id", Integer.parseInt(values[2]))
                .setUdtValue("name", getCustomerName(values[3], values[4], values[5]))
                .setBigDecimal("bal", new BigDecimal(values[16]))
                .setFloat("ytd", Float.parseFloat(values[17]))
                .setInt("p_cnt", Integer.parseInt(values[18]))
                .setInt("d_cnt", Integer.parseInt(values[19]))
                .build()
            );
            ++insertCustomerLineCount;

            if (insertCustomerLineCount % 3000 == 0) {
                session.execute(insertCustomerStatementBatch.build());
                insertCustomerStatementBatch.clearStatements();
                session.execute(insertCustomerOrderStatsBatch.build());
                insertCustomerOrderStatsBatch.clearStatements();
            }
        } catch (ParseException e) {
            //TODO: Add log
            System.out.println(e.getLocalizedMessage());
        }
        
    }

    public void insertCustomerDistrict(String line) {
        String[] values = line.split(",");
        session.execute(insertDistrictStmt.boundStatementBuilder()
            .setString("name", values[2])
            .setUdtValue("address", getAddress(values[3], values[4], values[5], values[6], values[7]))
            .setBigDecimal("tax", new BigDecimal(values[8]))
            .setInt("o_id", Integer.parseInt(values[10]))
            .setInt("w_id", Integer.parseInt(values[0]))
            .setInt("d_id", Integer.parseInt(values[1]))
            .build());
        
        session.execute(String.format("UPDATE %s SET D_%s_YTD = %s WHERE W_ID = %s", TableName.CUSTOMER_ORDER_STATS, values[1], new BigDecimal(values[9]), values[0]));
    }

    public void insertCustomerWarehouse(String line) {
        String[] values = line.split(",");
        for (int i = 1; i <= Constants.NUM_DISTRICTS; i += 1) {
            session.execute(insertWarehouseStmt.boundStatementBuilder()
                .setString("name", values[1])
                .setUdtValue("address", getAddress(values[2], values[3], values[4], values[5], values[6]))
                .setBigDecimal("tax", new BigDecimal(values[7]))
                .setInt("w_id", Integer.parseInt(values[0]))
                .setInt("d_id", i)
                .build());
        }
    }

    public void insertItem(String line) {
        String[] values = line.split(",");

        BoundStatement insertData = insertItemsStmt.bind(1 , Integer.parseInt(values[0]),
                values[1], BigDecimal.valueOf(Double.parseDouble(values[2])),  Integer.parseInt(values[3]) , values[4]);

        session.execute(insertData);
    }

    private DataLoader(CqlSession session, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;
        getUserDefinedTypes();
        createPreparedStatements();
    }

    private void createPreparedStatements() {
        String customerDataTableName = String.format("%s.%s", keyspace, TableName.CUSTOMER_DATA);
        String itemTableName = String.format("%s.%s", keyspace, TableName.ITEM);

        insertCustomerStmt = session.prepare(
            "INSERT INTO " + customerDataTableName + " (W_ID, D_ID, C_ID, C_NAME, C_ADDRESS, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM," + 
            " C_DISCOUNT, C_DATA) values (:w_id, :d_id, :c_id, :name, :address, :phone, :since, :credit, :lim, :dis, :data)"
        );

        insertDistrictStmt = session.prepare(
            "UPDATE " + customerDataTableName + " SET D_NAME = :name, D_ADDRESS = :address, D_TAX = :tax, D_NEXT_O_ID = :o_id where W_ID = :w_id and D_ID = :d_id"
        );

        insertWarehouseStmt = session.prepare(String.format(
            "UPDATE " + customerDataTableName + " SET W_NAME = :name, W_ADDRESS = :address, W_TAX =: tax where W_ID = :w_id and D_ID = :d_id"
            )
        );

        insertCustomerOrderStatsStmt = session.prepare(
            "INSERT INTO " + String.format("%s.%s", keyspace, TableName.CUSTOMER_ORDER_STATS) + " (W_ID, D_ID, C_ID, C_NAME, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT) " + 
            "values (:w_id, :d_id, :c_id, :name, :bal, :ytd, :p_cnt, :d_cnt)"
        );


        insertItemsStmt = session.prepare(
                "INSERT INTO " + itemTableName + " (I , I_ID , I_NAME , I_PRICE, I_IM_ID ,I_DATA)"
                        + "VALUES (:I , :I_ID , :I_NAME, :I_PRICE , :I_IM_ID, :I_DATA)"
        );

       /* String orderTableName = String.format("%s.%s" , keyspace , TableName.ORDER);
        String stockTableName = String.format("%s.%s", keyspace, TableName.STOCK);
        insertStocksStmt = session.prepare(
                "INSERT INTO " + stockTableName + " (S_W_ID , S_I_ID , S_QUANTITY , S_YTD , S_ORDER_CNT , " +
                "S_REMOTE_CNT , S_DIST_1 , S_DIST_2 , S_DIST_3 , S_DIST_4, S_DIST_5, S_DIST_6 , S_DIST_7, S_DIST_8, S_DIST_9 , S_DIST_10 , S_DATA)"
                + "VALUES (:S_W_ID, :S_I_ID, :S_QUANTITY , :S_YTD, :S_ORDER_CNT , :S_REMOTE_CNT, :S_DIST_1, :S_DIST_2 , :S_DIST_3 , :S_DIST_4, " +
                ":S_DIST_5, :S_DIST_6 , :S_DIST_7, :S_DIST_8, :S_DIST_9 , :S_DIST_10 , :S_DATA)"
        );
        insertOrderStmt = session.prepare("INSERT INTO " + orderTableName + " (O_W_ID , O_D_ID, O_ID, O_C_ID, O_CARRIER_ID , " +
                "O_OL_CNT , O_ALL_LOCAL, O_ENTRY_D , TOTAL_AMOUNT, ITEM_IDS , ORDER_LINES )"
                + "VALUES (:O_W_ID , :O_D_ID, :O_ID, :O_C_ID, :O_CARRIER_ID ," +
                ":O_OL_CNT , :O_ALL_LOCAL, :O_ENTRY_D , :TOTAL_AMOUNT , :ITEM_IDS ,:ORDER_LINES)");*/
    }

    private void getUserDefinedTypes() {
        customerNameUdt = session.getMetadata().getKeyspace(keyspace)
        .get().getUserDefinedType(Udt.CUSTOMER_NAME).get();
        addressUdt = session.getMetadata().getKeyspace(keyspace)
        .get().getUserDefinedType(Udt.ADDRESS).get();
        orderLineUdt = session.getMetadata().getKeyspace(keyspace)
                .get().getUserDefinedType(Udt.ORDER_LINE).get();
    }

    public UdtValue getAddress(String street1, String street2, String city, String state, String zip) {
        return addressUdt.newValue()
            .setString(Address.STREET_1, street1)
            .setString(Address.STREET_2, street2)
            .setString(Address.CITY, city)
            .setString(Address.STATE, state)
            .setString(Address.ZIP, zip);
    }

    private UdtValue getCustomerName(String firstName, String middleName, String lastName) {
        return customerNameUdt.newValue()
            .setString("FIRST_NAME", firstName)
            .setString("MIDDLE_NAME", middleName)
            .setString("LAST_NAME", lastName);
    }

     /*public void insertStock(String line){
        String[] values = line.split(",");

        BoundStatement insertData = insertStocksStmt.bind(Integer.parseInt(values[0]),
                Integer.parseInt(values[1]), Integer.parseInt(values[2]) , BigDecimal.valueOf(Double.parseDouble(values[3])) ,
                Integer.parseInt(values[4]) , Integer.parseInt(values[5]) , values[6] , values[7] , values[8] , values[9],
                values[10] , values[11] , values[12] , values[13] , values[14] , values[15] , values[16]);

        session.execute(insertData);
    }*/

    /*public void insertOrder(String line) {
        try {
            String[] values = line.split("\",");
            List<UdtValue> udtOrderlines = new ArrayList<UdtValue>();
            List<Integer> itemIDs = new ArrayList<>();
            BigDecimal totalAmount = new BigDecimal(0);
            String temp_data = values[1].replaceAll("\"", "");

            //This build the ITEM_IDs list and the udt orderlines list
            JSONArray jsonArray = new JSONArray(temp_data);
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObj = jsonArray.getJSONObject(i);
                udtOrderlines.add(getOrderLineUDTFromJSON(jsonObj));
                itemIDs.add(jsonObj.getInt("ol_i_id"));
                totalAmount = totalAmount.add(BigDecimal.valueOf(jsonObj.getDouble("ol_amount")));
            }

            String[] second_split = values[0].split(",\"");
            String[] order_values = second_split[0].split(",");

            BoundStatement insertData = insertOrderStmt.bind(Integer.parseInt(order_values[0]),
                    Integer.parseInt(order_values[1]), Integer.parseInt(order_values[2]), Integer.parseInt(order_values[3]), Integer.parseInt(order_values[4]),
                    BigDecimal.valueOf(Double.parseDouble(order_values[5])), BigDecimal.valueOf(Double.parseDouble(order_values[6])), DATE_FORMAT.parse(order_values[7]).toInstant(), totalAmount, itemIDs, udtOrderlines);

            session.execute(insertData);

        }catch (JSONException | ParseException e) {
            System.out.println(e.getLocalizedMessage());
        }
    }

     public UdtValue getOrderLineUDTFromJSON(JSONObject jsonObject) throws JSONException {
        return orderLineUdt.newValue()
                .setInt("OL_NUMBER", jsonObject.getInt("ol_number"))
                .setInt("OL_I_ID", jsonObject.getInt("ol_i_id"))
                .setInstant("OL_DELIVERY_D", null)
                .setBigDecimal("OL_AMOUNT", BigDecimal.valueOf(jsonObject.getDouble("ol_amount")))
                .setInt("OL_SUPPLY_W_ID", jsonObject.getInt("ol_supply_w_id"))
                .setBigDecimal("OL_QUANTITY", BigDecimal.valueOf(jsonObject.getDouble("ol_quantity")));
    }*/
}
