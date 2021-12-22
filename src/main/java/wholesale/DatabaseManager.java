package wholesale;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import wholesale.Constants.*;
import wholesale.Util.FileLineReader;

public final class DatabaseManager {
    
    private CqlSession session;
    private String keyspace;

    private static DatabaseManager INSTANCE;

    public static void setup(CqlSession session, String keyspace) {
        DatabaseManager manager = getInstance(session, keyspace);
        manager.createKeySpace();
        manager.createUdts();
        manager.createTables();
    }

    public static void loadData(CqlSession session, String keyspace) {
        DatabaseManager manager = getInstance(session, keyspace);
        manager.loadCustomerData();
    }

    public void createKeySpace() {
        session.execute(SchemaBuilder.dropKeyspace(keyspace).ifExists().build());
        ResultSet result = session.execute(SchemaBuilder.createKeyspace(keyspace).ifNotExists()
            .withSimpleStrategy(3)
            .build());
        logCreateResult("Keyspace", keyspace, result.wasApplied());
    }

    public void createUdts() {
        createOrderLineType();
        createAddressUdt();
        createCustomerNameUdt();
    }

    public void createTables() {
        createOrderTable();
        createOrderCarrierTable();
        createCustomerDataTable();
        createCustomerOrderStatsTable();
        createCustomerBalanceTable();
        createStockTable();
        createItemTable();
        createMatViewOrderTable();
    }

    public void createOrderLineType() {
        session.execute(SchemaBuilder.dropType(keyspace, Udt.ORDER_LINE).ifExists().build());
        ResultSet result = session.execute(SchemaBuilder.createType(keyspace, Udt.ORDER_LINE).ifNotExists()
                .withField("OL_NUMBER", DataTypes.INT)
                .withField("OL_I_ID", DataTypes.INT)
                .withField("OL_AMOUNT", DataTypes.DECIMAL)
                .withField("OL_SUPPLY_W_ID", DataTypes.INT)
                .withField("OL_QUANTITY", DataTypes.DECIMAL)
                .build());
        logCreateUdtResult(Udt.ORDER_LINE, result.wasApplied());
    }

    public void createAddressUdt() {
        session.execute(SchemaBuilder.dropType(keyspace, Udt.ADDRESS).ifExists().build());
        ResultSet result = session.execute(SchemaBuilder.createType(keyspace, Udt.ADDRESS)
            .ifNotExists()
            .withField(Address.STREET_1, DataTypes.TEXT)
            .withField(Address.STREET_2, DataTypes.TEXT)
            .withField(Address.CITY, DataTypes.TEXT)
            .withField(Address.STATE, DataTypes.TEXT)
            .withField(Address.ZIP, DataTypes.TEXT)
            .build());
        logCreateUdtResult(Udt.ADDRESS, result.wasApplied());
    }

    public void createCustomerNameUdt() {
        session.execute(SchemaBuilder.dropType(keyspace, Udt.CUSTOMER_NAME).ifExists().build());
        ResultSet result = session.execute(SchemaBuilder.createType(keyspace, Udt.CUSTOMER_NAME)
            .ifNotExists()
            .withField(CustomerName.FIRST_NAME, DataTypes.TEXT)
            .withField(CustomerName.MIDDLE_NAME, DataTypes.TEXT)
            .withField(CustomerName.LAST_NAME, DataTypes.TEXT)
            .build());
        logCreateUdtResult(Udt.CUSTOMER_NAME, result.wasApplied());
    }

    public void createOrderTable() {
        session.execute(SchemaBuilder.dropTable(keyspace, TableName.ORDER).ifExists().build());
        ResultSet result = session.execute(SchemaBuilder.createTable(keyspace, TableName.ORDER).ifNotExists()
                .withPartitionKey("O_W_ID", DataTypes.INT)
                .withPartitionKey("O_D_ID", DataTypes.INT)
                .withClusteringColumn("O_C_ID", DataTypes.INT)
                .withClusteringColumn("O_ID", DataTypes.INT)
                .withStaticColumn("D_NEXT_O_ID", DataTypes.INT)
                .withColumn("O_OL_CNT", DataTypes.DECIMAL)
                .withColumn("O_ALL_LOCAL", DataTypes.DECIMAL)
                .withColumn("O_ENTRY_D", DataTypes.TIMESTAMP)
                .withColumn("TOTAL_OL_QUANTITY", DataTypes.INT)
                .withColumn("ITEM_IDS", DataTypes.listOf(DataTypes.INT))
                .withColumn("ORDER_LINES", DataTypes.listOf(SchemaBuilder.udt(Udt.ORDER_LINE, true), true))
                .build());
        logCreateTableResult(TableName.ORDER, result.wasApplied());
    }

    public void createOrderCarrierTable() {
        session.execute(SchemaBuilder.dropTable(keyspace, TableName.ORDER_CARRIER).ifExists().build());
        ResultSet result = session.execute(SchemaBuilder.createTable(keyspace, TableName.ORDER_CARRIER).ifNotExists()
                .withPartitionKey("O_W_ID", DataTypes.INT)
                .withClusteringColumn("O_D_ID", DataTypes.INT)
                .withClusteringColumn("O_ID", DataTypes.INT)
                .withClusteringColumn("O_C_ID", DataTypes.INT)
                .withColumn("O_CARRIER_ID", DataTypes.INT)
                .withColumn("TOTAL_AMOUNT", DataTypes.DECIMAL)
                .withColumn("OL_DELIVERY_D", DataTypes.TIMESTAMP)
                .build());
        logCreateTableResult(TableName.ORDER_CARRIER, result.wasApplied());
    }

    public void createMatViewOrderTable() {
        session.execute(SchemaBuilder.dropMaterializedView(keyspace, TableName.ORDER_MAT_VIEW).ifExists().build());
        ResultSet result = session.execute(SchemaBuilder.createMaterializedView( keyspace, TableName.ORDER_MAT_VIEW).ifNotExists()
                .asSelectFrom(keyspace, TableName.ORDER)
                .columns("O_W_ID" , "O_D_ID"  , "O_ID" ,  "O_C_ID", "O_ENTRY_D", "ITEM_IDS"  , "ORDER_LINES")
                .whereColumn("O_W_ID").isNotNull()
                .whereColumn("O_D_ID").isNotNull()
                .whereColumn("O_ID").isNotNull()
                .whereColumn("O_C_ID").isNotNull()
                .withPartitionKey("O_W_ID")
                .withPartitionKey("O_D_ID")
                .withClusteringColumn("O_ID")
                .withClusteringColumn("O_C_ID")
                .withClusteringOrder("O_ID", ClusteringOrder.DESC)
                .withClusteringOrder("O_C_ID", ClusteringOrder.ASC)
                .build());
        logCreateTableResult(TableName.ORDER_MAT_VIEW, result.wasApplied());
    }

    public void createCustomerDataTable() {
        session.execute(SchemaBuilder.dropTable(keyspace, TableName.CUSTOMER_DATA).ifExists().build());
        ResultSet result = session.execute(SchemaBuilder.createTable(keyspace, TableName.CUSTOMER_DATA)
            .ifNotExists()
            .withPartitionKey(Customer.WAREHOUSE_ID, DataTypes.INT)
            .withPartitionKey(Customer.DISTRICT_ID, DataTypes.INT)
            .withClusteringColumn(Customer.ID, DataTypes.INT)
            .withStaticColumn(Customer.WAREHOUSE_NAME, DataTypes.TEXT)
            .withStaticColumn(Customer.WAREHOUSE_ADDRESS, SchemaBuilder.udt(Udt.ADDRESS, true))
            .withStaticColumn(Customer.WAREHOUSE_TAX, DataTypes.DECIMAL)
            .withStaticColumn(Customer.DISTRICT_NAME, DataTypes.TEXT)
            .withStaticColumn(Customer.DISTRICT_ADDRESS, SchemaBuilder.udt(Udt.ADDRESS, true))
            .withStaticColumn(Customer.DISTRICT_TAX, DataTypes.DECIMAL)
            .withColumn(Customer.NAME, SchemaBuilder.udt(Udt.CUSTOMER_NAME, true))
            .withColumn(Customer.ADDRESS, SchemaBuilder.udt(Udt.ADDRESS, true))
            .withColumn(Customer.PHONE, DataTypes.TEXT)
            .withColumn(Customer.SINCE, DataTypes.TIMESTAMP)
            .withColumn(Customer.CREDIT, DataTypes.TEXT)
            .withColumn(Customer.CREDIT_LIM, DataTypes.DECIMAL)
            .withColumn(Customer.DISCOUNT, DataTypes.DECIMAL)
            .withColumn(Customer.DATA, DataTypes.TEXT)
            .withCompaction(SchemaBuilder.leveledCompactionStrategy())
            .withClusteringOrder(Customer.ID, ClusteringOrder.ASC)
            .build()
        );
        logCreateTableResult(TableName.CUSTOMER_DATA, result.wasApplied());
    }

    public void createCustomerOrderStatsTable() {
        session.execute(SchemaBuilder.dropTable(keyspace, TableName.CUSTOMER_ORDER_STATS).ifExists().build());
        CreateTable createCustomerTable = SchemaBuilder.createTable(keyspace, TableName.CUSTOMER_ORDER_STATS)
            .ifNotExists()
            .withPartitionKey(Customer.WAREHOUSE_ID, DataTypes.INT)
            .withClusteringColumn(Customer.DISTRICT_ID, DataTypes.INT)
            .withClusteringColumn(Customer.ID, DataTypes.INT)
            .withColumn(Customer.NAME, SchemaBuilder.udt(Udt.CUSTOMER_NAME, true))
            .withColumn(Customer.BALANCE, DataTypes.DECIMAL)
            .withColumn(Customer.YTD_PAYMENT, DataTypes.FLOAT)
            .withColumn(Customer.PAYMENT_CNT, DataTypes.INT)
            .withColumn(Customer.DELIVERY_CNT, DataTypes.INT);

        for (int i=1; i <= Constants.NUM_DISTRICTS; i++) {
            createCustomerTable = createCustomerTable.withStaticColumn(String.format("D_%d_YTD", i), DataTypes.DECIMAL);
        }

        ResultSet result = session.execute(createCustomerTable.withClusteringOrder(Customer.DISTRICT_ID, ClusteringOrder.ASC)
            .withClusteringOrder(Customer.ID, ClusteringOrder.ASC)
            .build());
        logCreateTableResult(TableName.CUSTOMER_ORDER_STATS, result.wasApplied());
    }

    public void createCustomerBalanceTable() {
        session.execute(SchemaBuilder.dropMaterializedView(keyspace, TableName.CUSTOMER_BALANCE).ifExists().build()); 
        ResultSet result = session.execute(SchemaBuilder.createMaterializedView(keyspace, TableName.CUSTOMER_BALANCE)
            .ifNotExists()
            .asSelectFrom(TableName.CUSTOMER_ORDER_STATS)
            .columns(Customer.WAREHOUSE_ID, Customer.DISTRICT_ID, Customer.ID, Customer.BALANCE)
            .whereColumn(Customer.WAREHOUSE_ID).isNotNull()
            .whereColumn(Customer.DISTRICT_ID).isNotNull()
            .whereColumn(Customer.ID).isNotNull()
            .whereColumn(Customer.BALANCE).isNotNull()
            .withPartitionKey(Customer.WAREHOUSE_ID)
            .withClusteringColumn(Customer.BALANCE)
            .withClusteringColumn(Customer.DISTRICT_ID)
            .withClusteringColumn(Customer.ID)
            .withClusteringOrder(Customer.BALANCE, ClusteringOrder.DESC)
            .withClusteringOrder(Customer.DISTRICT_ID, ClusteringOrder.ASC)
            .withClusteringOrder(Customer.ID, ClusteringOrder.ASC)
            .build()
        );
        logCreateTableResult(TableName.CUSTOMER_BALANCE, result.wasApplied()); 
    }

    public void createStockTable(){
        session.execute(SchemaBuilder.dropTable(keyspace, TableName.STOCK).ifExists().build());
        ResultSet result = session.execute(SchemaBuilder.createTable(keyspace, TableName.STOCK)
                .withPartitionKey("S_W_ID", DataTypes.INT)
                .withClusteringColumn("S_I_ID", DataTypes.INT)
                .withColumn("S_QUANTITY", DataTypes.INT)
                .withColumn("S_YTD", DataTypes.DECIMAL)
                .withColumn("S_ORDER_CNT", DataTypes.INT)
                .withColumn("S_REMOTE_CNT", DataTypes.INT)
                .withColumn("S_DIST_1", DataTypes.TEXT)
                .withColumn("S_DIST_2", DataTypes.TEXT)
                .withColumn("S_DIST_3", DataTypes.TEXT)
                .withColumn("S_DIST_4", DataTypes.TEXT)
                .withColumn("S_DIST_5", DataTypes.TEXT)
                .withColumn("S_DIST_6", DataTypes.TEXT)
                .withColumn("S_DIST_7", DataTypes.TEXT)
                .withColumn("S_DIST_8", DataTypes.TEXT)
                .withColumn("S_DIST_9", DataTypes.TEXT)
                .withColumn("S_DIST_10", DataTypes.TEXT)
                .withColumn("S_DATA", DataTypes.TEXT)
                .build());
        logCreateTableResult(TableName.STOCK, result.wasApplied());
    }

    public void createItemTable(){
        session.execute(SchemaBuilder.dropTable(keyspace, TableName.ITEM).ifExists().build());
        ResultSet result = session.execute(SchemaBuilder.createTable(keyspace , TableName.ITEM)
                .withPartitionKey("I" , DataTypes.INT)
                .withClusteringColumn("I_ID" , DataTypes.INT)
                .withColumn("I_NAME" , DataTypes.TEXT)
                .withColumn("I_PRICE" , DataTypes.DECIMAL)
                .withColumn("I_IM_ID" , DataTypes.INT)
                .withColumn("I_DATA" , DataTypes.TEXT)
                .build());
        logCreateTableResult(TableName.ITEM, result.wasApplied());
    }

    public void loadCustomerData() {
        DataLoader dataLoader = DataLoader.getInstance(session, keyspace);
        Util.readFile(DataFilePath.CUSTOMER, new FileLineReader() {

            int count = 0;

            @Override
            public void read(String line) {
                dataLoader.insertCustomer(line);
                ++count;
                if (count % 1000 == 0) {
                    logDataLoadProgress("Customer", count);
                }
            }
        });

        Util.readFile(DataFilePath.DISTRICT, new FileLineReader() {

            int count = 0;

            @Override
            public void read(String line) {
                dataLoader.insertCustomerDistrict(line);

                ++count;
                if (count % 1000 == 0) {
                    logDataLoadProgress("Customer.District", count);
                }
            }
        });

        Util.readFile(DataFilePath.WAREHOUSE, new FileLineReader() {

            int count = 0;

            @Override
            public void read(String line) {
                dataLoader.insertCustomerWarehouse(line);

                ++count;
                if (count % 1000 == 0) {
                    logDataLoadProgress("Customer.Warehouse", count);
                }
            }
        });

        Util.readFile(DataFilePath.ITEM, new FileLineReader() {
            int count = 0;
            @Override
            public void read(String line) {
                dataLoader.insertItem(line);

                ++count;
                if (count % 1000 == 0) {
                    logDataLoadProgress("item" , count);
                }
            }
        });

        /*Util.readFile(DataFilePath.STOCK, new FileLineReader() {
            int count = 0;
            @Override
            public void read(String line) {
                dataLoader.insertStock(line);

                ++count;
                if (count % 1000 == 0) {
                 logDataLoadProgress("stock" , count);
                }
            }
        });

        Util.readFile(DataFilePath.ORDER, new FileLineReader() {
            int count = 0;
            @Override
            public void read(String line) {
                dataLoader.insertOrder(line);

                ++count;
                if (count % 1000 == 0) {
                    logDataLoadProgress("order" , count);
                }
            }
        });*/
    }

    private static DatabaseManager getInstance(CqlSession session, String keyspaceName) {
        if (INSTANCE == null) {
            INSTANCE = new DatabaseManager(session, keyspaceName);
        }
        return INSTANCE;
    }

    private DatabaseManager(CqlSession session, String keyspaceName) {
        this.session = session;
        this.keyspace = keyspaceName;
    }

    private static void logCreateTableResult(String tableName, boolean success) {
        logCreateResult("Table", tableName, success);
    }

    private static void logCreateUdtResult(String udtName, boolean success) {
        logCreateResult("UDT", udtName, success);
    }

    private static void logCreateResult(String type, String name, boolean success) {
        Util.debugLog(DatabaseManager.class.getSimpleName(), 
                    String.format("Create %s: %s %s", type, name, success ? "success" : "failure"));
    }

    private static void logDataLoadProgress(String table, int count) {
        Util.debugLog(DatabaseManager.class.getSimpleName(), String.format("%s: processed %d rows", table, count));
    }
}
