package wholesale;
final class Constants {

    private Constants() {

    }

    public static final int NUM_DISTRICTS = 10;
    public static final String KEYSPACE_NAME = "wholesale_supplier";

    public static interface Udt {
        String ADDRESS = "address";
        String CUSTOMER_NAME = "customer_name";
        String ORDER_LINE = "order_line";
    }

    public static interface TableName {
        String CUSTOMER_DATA = "customer_data";
        String CUSTOMER_ORDER_STATS = "customer_order_stats";
        String CUSTOMER_BALANCE = "customer_balance";
        String ORDER = "order_table";
        String ORDER_CARRIER = "order_carrier"; 
        String STOCK = "stock";
        String ITEM = "item";
        String ORDER_MAT_VIEW = "order_table_mat_view";
    }

    public static interface Address {
        String STREET_1 = "STREET_1";
        String STREET_2 = "STREET_2";
        String CITY = "CITY";
        String STATE = "STATE";
        String ZIP = "ZIP";
    }

    public static interface CustomerName {
        String FIRST_NAME = "FIRST_NAME";
        String MIDDLE_NAME = "MIDDLE_NAME";
        String LAST_NAME = "LAST_NAME";
    }

    public static interface Customer {
        // Warehouse
        String WAREHOUSE_ID = "C_W_ID";
        String WAREHOUSE_NAME = "W_NAME";
        String WAREHOUSE_TAX = "W_TAX";
        String WAREHOUSE_YTD = "W_YTD";
        String WAREHOUSE_ADDRESS = "W_ADDRESS";

        // District
        String DISTRICT_ID = "C_D_ID";
        String DISTRICT_NAME = "D_NAME";
        String DISTRICT_TAX = "D_TAX";
        String DISTRICT_YTD = "D_YTD";
        String DISTRICT_ADDRESS = "D_ADDRESS";

        // Customer
        String NAME = "C_NAME";
        String ID = "C_ID";
        String ADDRESS = "C_ADDRESS";
        String PHONE = "C_PHONE";
        String SINCE = "C_SINCE";
        String CREDIT = "C_CREDIT";
        String BALANCE = "C_BALANCE";
        String CREDIT_LIM = "C_CREDIT_LIM";
        String DISCOUNT = "C_DISCOUNT";
        String YTD_PAYMENT = "C_YTD_PAYMENT";
        String PAYMENT_CNT = "C_PAYMENT_CNT";
        String DELIVERY_CNT = "C_DELIVERY_CNT";
        String DATA = "C_DATA";
    }
    public static interface Order {
        String WAREHOUSE_ID = "o_w_id";
        String DISTRICT_ID = "o_d_id";
        String CUSTOMER_ID = "o_c_id";
        String ORDER_ID = "o_id";
        String ENTRY_DATE = "o_entry_d";
        String CARRIER_ID = "o_carrier_id";
        String ORDER_LINES = "order_lines";
        String DELIVERY_DATE = "ol_delivery_d";
        String D_NEXT_ORDER_ID = "D_NEXT_O_ID";
        String ORDERLINE_COUNT= "O_OL_CNT";
        String ALL_COUNT = "O_ALL_LOCAL";
        String TOTAL_AMOUNT = "TOTAL_AMOUNT";
        String ITEM_IDS = "ITEM_IDS";
        String TOTAL_OL_QUANTITY = "TOTAL_OL_QUANTITY";
    }
    public static interface  OrderLine {
        String ITEM_ID = "ol_i_id";
        String SUPPLY_W_ID = "OL_SUPPLY_W_ID";
        String QUANTITY = "OL_QUANTITY";
        String AMOUNT = "OL_AMOUNT";
        String NUMBER = "OL_NUMBER";
    }
    public static interface Item {
        String ITEM_ID = "I_ID";
        String PRICE = "I_PRICE";
        String NAME = "I_NAME";

    }

    public static final String DATA_DIRECTORY = "cassandra/data";

    public static interface DataFilePath {
        String CUSTOMER = DATA_DIRECTORY + "/customer.csv";
        String DISTRICT = DATA_DIRECTORY + "/district.csv";
        String WAREHOUSE = DATA_DIRECTORY + "/warehouse.csv";
        String STOCK = DATA_DIRECTORY + "/stock.csv";
        String ITEM = DATA_DIRECTORY + "/item.csv";
        String ORDER = DATA_DIRECTORY + "/order.csv";
    }

    public static final String METRICS_DIR = "metrics";
    public static final String TRANSACTION_DIR = "transactions";

}