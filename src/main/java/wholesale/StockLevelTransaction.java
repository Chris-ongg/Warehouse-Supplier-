package wholesale;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import wholesale.Constants.TableName;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StockLevelTransaction {
    private CqlSession session;
    private int stockThreshold;
    private ResultSet searchResult;
    private PreparedStatement getOrders;
    private PreparedStatement stockQuery;

    public StockLevelTransaction(CqlSession session ) {
        this.session = session;
        //Prepare query to get first N orders from materialized view order table. (table is arranged in DESC order_id)
        this.getOrders = session.prepare(
                QueryBuilder.selectFrom(TableName.ORDER_MAT_VIEW).columns("ITEM_IDS")
                        .whereColumn("O_W_ID").isEqualTo(QueryBuilder.bindMarker("W_ID"))
                        .whereColumn("O_D_ID").isEqualTo(QueryBuilder.bindMarker("D_ID"))
                        .limit(QueryBuilder.bindMarker("orderRange"))
                        .build().setConsistencyLevel(ConsistencyLevel.ONE)
        );
        //Prepare query to get item stock info from stock table
        this.stockQuery = session.prepare(
                QueryBuilder.selectFrom(TableName.STOCK).columns("S_QUANTITY")
                        .whereColumn("S_W_ID").isEqualTo(QueryBuilder.bindMarker("W_ID"))
                        .whereColumn("S_I_ID").isEqualTo(QueryBuilder.bindMarker("I_ID"))
                        .build().setConsistencyLevel(ConsistencyLevel.ONE)
        );
    }

    public void executeTransaction(int W_ID, int D_ID  , int stockThreshold , int orderRange){
        this.searchResult = null;
        this.stockThreshold = stockThreshold;
        Set<Integer> items = getAllItemInOrders(this.getOrders ,  W_ID, D_ID , orderRange);
        checkForItemBelowStockThreshold( items, W_ID);
    }

    public Set<Integer> getAllItemInOrders(PreparedStatement ordersQuery , int W_ID , int D_ID , int orderRange){
        //Step 1: Get latest order number from district table
        ResultSet res = session.execute(ordersQuery.bind().setInt("W_ID" , W_ID).setInt("D_ID" , D_ID).setInt("orderRange" , orderRange));
        Set<Integer> items = new HashSet<>();
        List <Integer> itemsId = new ArrayList<Integer>();
        //Step 2: Get all the items purchased within all the orders number within a specified range
        for (Row order: res) {
            itemsId = order.getList("ITEM_IDS" , Integer.class);
            items.addAll(itemsId);
        }
        return items;
    }

    public void checkForItemBelowStockThreshold( Set<Integer> items , int W_ID){
        //Step 3: print out all the items below the specified stock threshold
        for (int itemID: items) {
            ResultSet temp = session.execute(this.stockQuery.bind()
                    .setInt("W_ID" , W_ID)
                    .setInt("I_ID" , itemID));
            int stockQuantity = temp.one().getInt("S_QUANTITY");
            if (stockQuantity < this.stockThreshold) {
                System.out.println("ItemID: " + itemID + " has a stock quantity of " + stockQuantity);
            }
        }
    }
}
