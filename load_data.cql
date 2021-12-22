COPY wholesale_supplier.customer_data (C_W_ID,C_D_ID,C_ID,C_ADDRESS,C_CREDIT,C_CREDIT_LIM,C_DATA,C_DISCOUNT,C_NAME,C_PHONE,C_SINCE,D_ADDRESS,D_NAME,D_TAX,W_ADDRESS,W_NAME,W_TAX) FROM './data/customer_data.csv';
COPY wholesale_supplier.customer_order_stats (C_W_ID,C_D_ID,C_ID,C_BALANCE,C_DELIVERY_CNT,C_NAME,C_PAYMENT_CNT,C_YTD_PAYMENT,D_10_YTD,D_1_YTD,D_2_YTD,D_3_YTD,D_4_YTD,D_5_YTD,D_6_YTD,D_7_YTD,D_8_YTD,D_9_YTD) FROM './data/customer_order_stats.csv';

COPY wholesale_supplier.order_table (O_W_ID,O_D_ID,O_C_ID,O_ID,D_NEXT_O_ID,O_OL_CNT,O_ALL_LOCAL,O_ENTRY_D,TOTAL_OL_QUANTITY,ITEM_IDS,ORDER_LINES) FROM './data/order.csv';
COPY wholesale_supplier.order_carrier (O_W_ID,O_D_ID,O_ID,O_C_ID,O_CARRIER_ID,TOTAL_AMOUNT,OL_DELIVERY_D) FROM './data/order_carrier.csv';

COPY wholesale_supplier.stock (S_W_ID,S_I_ID,S_DATA,S_DIST_1,S_DIST_10,S_DIST_2,S_DIST_3,S_DIST_4,S_DIST_5,S_DIST_6,S_DIST_7,S_DIST_8,S_DIST_9,S_ORDER_CNT,S_QUANTITY,S_REMOTE_CNT,S_YTD) FROM './data/stock.csv';

COPY wholesale_supplier.item (I,I_ID,I_NAME,I_PRICE,I_IM_ID,I_DATA) FROM './data/item.csv';