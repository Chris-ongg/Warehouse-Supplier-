package wholesale;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.schema.CreateType;


public class CreateTables {
    private CqlSession session;
    private String keySpace;

    public CreateTables(CqlSession session , String keySpace ) {
        this.session = session;
        this.keySpace = keySpace;
    }

    public void createOrderTable() {
        createOrderLineType(session);
        CreateTableWithOptions orderTable = SchemaBuilder.createTable(keySpace, "order_table").ifNotExists()
                .withPartitionKey("O_W_ID", DataTypes.INT)
                .withPartitionKey("O_D_ID", DataTypes.INT)
                .withClusteringColumn("O_C_ID", DataTypes.INT)
                .withClusteringColumn("O_ID", DataTypes.INT)
                .withColumn("O_CARRIER_ID", DataTypes.INT)
                .withColumn("O_OL_CNT", DataTypes.DECIMAL)
                .withColumn("O_ALL_LOCAL", DataTypes.DECIMAL)
                .withColumn("O_ENTRY_D", DataTypes.TIMESTAMP)
                .withColumn("TOTAL_AMOUNT", DataTypes.DECIMAL)
                .withColumn("ITEM_IDS", DataTypes.listOf(DataTypes.INT))
                .withColumn("ORDER_LINES", DataTypes.listOf(SchemaBuilder.udt("order_line_type", true), true));
        session.execute(orderTable.build());
    }
    public void createItemTable() {
        CreateTableWithOptions itemTable = SchemaBuilder.createTable(keySpace, "items").ifNotExists()
                .withPartitionKey("I_ID", DataTypes.INT)
                .withColumn("I_NAME", DataTypes.TEXT)
                .withColumn("I_PRICE", DataTypes.DECIMAL)
                .withColumn("I_IM_ID", DataTypes.INT)
                .withColumn("I_DATA", DataTypes.TEXT);
        session.execute(itemTable.build());
    }
    public void createStockTable(){
        CreateTableWithOptions stockTable = SchemaBuilder.createTable(keySpace, "stock").ifNotExists()
                .withPartitionKey("S_W_ID", DataTypes.INT)
                .withPartitionKey("S_I_ID", DataTypes.INT)
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
                .withColumn("S_DATA", DataTypes.TEXT);

        session.execute(stockTable.build());
    }
    public void createOrderLineType(CqlSession session) {
        CreateType orderLineType = SchemaBuilder.createType(keySpace, "order_line_type").ifNotExists()
                .withField("OL_NUMBER", DataTypes.INT)
                .withField("OL_I_ID", DataTypes.INT)
                .withField("OL_AMOUNT", DataTypes.DECIMAL)
                .withField("OL_SUPPLY_W_ID", DataTypes.INT)
                .withField("OL_QUANTITY", DataTypes.DECIMAL)
                .withField("OL_DELIVERY_D", DataTypes.TIMESTAMP);
        session.execute(orderLineType.build());
    }
}
