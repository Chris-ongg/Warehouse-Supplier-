package wholesale;


import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import wholesale.Constants.TableName;

import java.util.*;

public class PopularItemsTransaction {
    private CqlSession session;
    private int orderNumRange;
    private Hashtable<Integer , String> itemsNameMap;
    //Store the appearance count of each items in all orders
    private Hashtable<String , Integer>  itemsInOrders;
    //Store the most popular item(s) in every order
    private List<String> popularItems;
    //Below 3 hold string lists for printout
    private List<String> customerList;
    private List<String> orderInfoList;
    private List<String> popularItemsList;
    private ResultSet searchResult;

    private PreparedStatement getOrders;
    private PreparedStatement customerInfoQuery;
    private PreparedStatement itemQuery;

    public PopularItemsTransaction(CqlSession session){
        this.session = session;
        // Prepare query to get last N orders from materialized view order table. (Orders in arranged in DESC order_id in table )
        this.getOrders =session.prepare(
                QueryBuilder.selectFrom(TableName.ORDER_MAT_VIEW).columns("O_C_ID" , "O_ID",  "O_ENTRY_D","ORDER_LINES")
                        .whereColumn("O_W_ID").isEqualTo(QueryBuilder.bindMarker("W_ID"))
                        .whereColumn("O_D_ID").isEqualTo(QueryBuilder.bindMarker("D_ID"))
                        .limit(QueryBuilder.bindMarker("orderRange"))
                        .build().setConsistencyLevel(ConsistencyLevel.ONE));
        // Prepare query to get customer info from customerTable (customer_data table)
        this.customerInfoQuery = session.prepare(
                QueryBuilder.selectFrom(TableName.CUSTOMER_DATA).columns("C_NAME") //get cust name col only
                        .whereColumn("C_W_ID").isEqualTo(QueryBuilder.bindMarker("W_ID"))
                        .whereColumn("C_D_ID").isEqualTo(QueryBuilder.bindMarker("D_ID"))
                        .whereColumn("C_ID").isEqualTo(QueryBuilder.bindMarker("C_ID"))
                        .build().setConsistencyLevel(ConsistencyLevel.ONE)
        );
        //Prepare query to get item name from itemtable;
        this.itemQuery = session.prepare(
                QueryBuilder.selectFrom(TableName.ITEM).column("I_NAME")
                        .whereColumn("I").isEqualTo(QueryBuilder.literal(1))
                        .whereColumn("I_ID").isEqualTo(QueryBuilder.bindMarker("ID"))
                        .build().setConsistencyLevel(ConsistencyLevel.ONE)
        );
    }

    public void executeTransaction(int W_ID, int D_ID, int orderNumRange){
        this.itemsNameMap = new Hashtable<Integer, String>();
        this.itemsInOrders = new Hashtable<String , Integer>();;
        this.popularItems = new ArrayList<>();
        this.customerList = new ArrayList<>();
        this.orderInfoList = new ArrayList<>();
        this.popularItemsList = new ArrayList<>();
        this.searchResult = null;
        this.orderNumRange = orderNumRange;

        getAllPopularItemsInOrders( W_ID , D_ID );
        outputPopularItems();
    }


    public void setPrintOut(Row customerInfo , Row orderInfo , String itemPrintOut , int Id){
        String custName = customerInfo.getUdtValue("C_NAME").getFormattedContents();
        String time = orderInfo.getInstant("O_ENTRY_D").toString();
        String order_info = "Order id:" + Id + ",Timestamp:" + time;
        this.customerList.add(custName);
        this.orderInfoList.add(order_info);
        this.popularItemsList.add(itemPrintOut);
    }

    public void getAllPopularItemsInOrders( int W_ID , int D_ID){

        ResultSet all_orders = session.execute(this.getOrders.bind().setInt("W_ID" , W_ID).setInt("D_ID" , D_ID).setInt("orderRange" , this.orderNumRange));

        for (Row order: all_orders){
            int C_ID = order.getInt("O_C_ID");
            this.searchResult = session.execute(customerInfoQuery.bind()
                    .setInt("W_ID" , W_ID)
                    .setInt("D_ID" , D_ID)
                    .setInt("C_ID" , C_ID));
            Row customerInfo = this.searchResult.one();
            List<UdtValue> orderLines = order.getList("ORDER_LINES" , UdtValue.class);
            Hashtable<String , Integer> itemStats = new Hashtable<String , Integer>();
            for (UdtValue orderLine : orderLines){
                int Item_ID = orderLine.getInt("OL_I_ID");
                String itemName;
                //If item is present in map, get it from map instead of querying the item table.
                if (itemsNameMap.containsKey(Item_ID)){
                    itemName = itemsNameMap.get(Item_ID);
                }
                else {
                    ResultSet item = session.execute(itemQuery.bind().setInt("ID", Item_ID));
                    Row item_result = item.one();
                    itemName = item_result.getString("I_NAME");
                    //add item name to dictionary
                    itemsNameMap.put(Item_ID , itemName);
                }
                int quantityOrdered = orderLine.getBigDecimal("OL_QUANTITY").intValue();
                itemStats.put(itemName , quantityOrdered);
            }
            int maxValueInMap = Collections.max(itemStats.values());
            Set<String> setOfItems = itemStats.keySet();
            String itemPrintOut = "";
            for (String key: setOfItems) {
                // if item has the highest ordered quantity in order, add it to popular items
                // there might be a case where multiple popular items can appear (same quantity ordered value)
                if (itemStats.get(key) == maxValueInMap) {
                    if (!popularItems.contains(key)){
                        popularItems.add(key);
                    }
                    itemPrintOut = itemPrintOut +  "Pop Item:" + key +  ",Qty:" +  maxValueInMap + ",";
                }
                // if item appear in order, increment itemAppearance count. This is used to calculate popular item percentage later
                if (itemsInOrders.containsKey(key)) {
                    int currentCount = itemsInOrders.get(key);
                    itemsInOrders.put(key , currentCount + 1);
                }
                else {
                    itemsInOrders.put(key , 1);
                }
            }
            setPrintOut(customerInfo , order , itemPrintOut , order.getInt("O_ID"));
        }
    }

    public void outputPopularItems() {
        for (int i = 0 ; i < this.customerList.size(); i++){
            System.out.println(customerList.get(i));
            System.out.println(orderInfoList.get(i));
            System.out.println(popularItemsList.get(i));
        }
        for (String popular_items: popularItems){
            double percentage = ( itemsInOrders.get(popular_items)/ (double) this.orderNumRange) * 100.00;
            System.out.println("Pop item: " + popular_items + "," + " %: " + percentage);
        }

    }
}
