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

public class RelatedCustomerTransaction {
    private CqlSession session;

    private List<List<Integer>> customerOrders;
    private List<List<Integer>> relatedCustomer;
    private List<Row> all_cust_orders;
    private PreparedStatement allCustOrders;
    private ResultSet searchResult;

    public RelatedCustomerTransaction(CqlSession session){
        this.session = session;
        //Prepare query  to get all orders from the materialized view order table
        this.allCustOrders = session.prepare(QueryBuilder.selectFrom(TableName.ORDER_MAT_VIEW)
                .columns( "O_W_ID" , "O_D_ID" , "O_C_ID" , "ITEM_IDS" ).build());
    }

    public void executeTransaction(int W_ID , int D_ID , int C_ID){
        this.searchResult = null;
        this.customerOrders = new ArrayList<List<Integer>>();
        this.relatedCustomer = new ArrayList<List<Integer>>();;
        this.all_cust_orders = new ArrayList<>();

        buildListOfCustOrders( W_ID , D_ID , C_ID);
        getRelatedCustomer(W_ID , C_ID);
        outputRelatedCustomers();
    }

    public void buildListOfCustOrders( int W_ID , int D_ID , int C_ID){
        this.searchResult = session.execute(allCustOrders.bind().setConsistencyLevel(ConsistencyLevel.ONE));
        all_cust_orders = this.searchResult.all();
        for (Row order : all_cust_orders){
            //Get all the items in each order made by the customer.
            if ( (order.getInt("O_W_ID") == W_ID) & (order.getInt("O_D_ID") == D_ID) & (order.getInt("O_C_ID") == C_ID)) {
                this.customerOrders.add(order.getList("ITEM_IDS", Integer.class));
            }
        }
    }

    public void getRelatedCustomer(int W_ID  , int C_ID){
        //For each order in customerOrders compare against all other orders
        for (List<Integer> items : customerOrders){
            Set<Integer> setOfItems = new HashSet<Integer>(items);
            for (Row order: all_cust_orders){
                //Skip if same W_ID
                if (order.getInt("O_W_ID") == W_ID) {continue;}
                List<Integer> temp_items = order.getList("ITEM_IDS" , Integer.class);
                Set<Integer> setOfItems_ = new HashSet<Integer>(temp_items);
                //Compare against 2 sets and retain all similar items
                setOfItems_.retainAll(setOfItems);
                //If item count in set is more than 2, store the customer identifier in current order
                if (setOfItems_.size() >= 2) {
                    List <Integer> customerIdentifier = new ArrayList<>();
                    customerIdentifier.add(order.getInt("O_W_ID"));
                    customerIdentifier.add(order.getInt("O_D_ID"));
                    customerIdentifier.add(order.getInt("O_C_ID"));
                    this.relatedCustomer.add(customerIdentifier);
                }
            }
        }
    }

    public void outputRelatedCustomers(){
        System.out.println("Related Customer: ");
        for (List<Integer> related: this.relatedCustomer){
            System.out.println("O_W_ID :" + related.get(0)  + ", O_D_ID: " + related.get(1) + ", O_C_ID: " + related.get(2));
        }
    }

}

