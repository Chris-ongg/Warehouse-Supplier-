package wholesale;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class DbState {
 
    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder().build();) {

            session.execute("USE wholesale_supplier");

            BigDecimal c_balance_sum = new BigDecimal(0);
            BigDecimal c_ytd_payment_sum = new BigDecimal(0);
            long c_payment_cnt_sum = 0;
            long c_delivery_cnt_sum = 0;

            BigDecimal ol_amount_sum = new BigDecimal(0);

            BigDecimal d_ytd_sum = new BigDecimal(0);
            
            for (int w = 1 ; w <= 10; w++) {
                ResultSet result;
                Row row;
                result = session.execute("SELECT D_1_YTD, D_2_YTD, D_3_YTD, D_4_YTD, D_5_YTD, D_6_YTD, D_7_YTD, D_8_YTD, D_9_YTD, D_10_YTD, SUM(C_BALANCE) as C_BALANCE_SUM, SUM(C_YTD_PAYMENT) as C_YTD_PAYMENT_SUM, SUM(C_PAYMENT_CNT) as C_PAYMENT_CNT_SUM, SUM(C_DELIVERY_CNT) AS C_DELIVERY_CNT_SUM FROM customer_order_stats where c_w_id = " + w);
                row = result.one();
                c_balance_sum = c_balance_sum.add(row.getBigDecimal("C_BALANCE_SUM"));
                c_ytd_payment_sum = c_ytd_payment_sum.add(new BigDecimal(row.getFloat("C_YTD_PAYMENT_SUM")));
                c_payment_cnt_sum += row.getInt("C_PAYMENT_CNT_SUM");
                c_delivery_cnt_sum += row.getInt("C_DELIVERY_CNT_SUM");

                for (int d = 1; d <= 10; d++) {
                    d_ytd_sum = d_ytd_sum.add(row.getBigDecimal("D_" + d +"_YTD"));
                }

                result = session.execute("SELECT SUM(TOTAL_AMOUNT) as OL_AMOUNT_TOTAL FROM order_carrier where o_w_id = " + w);
                row = result.one();
                ol_amount_sum = ol_amount_sum.add(row.getBigDecimal("OL_AMOUNT_TOTAL"));
            }
            
            BigDecimal w_ytd = new BigDecimal(0).add(d_ytd_sum);


            BigDecimal count = new BigDecimal(0);
            long quantity = 0;
            int maxOid = Integer.MIN_VALUE;
            long nextOid = 0;
            for(int i=1;i<=10;i++) {
                for(int j=1;j<=10;j++) {
                    Row row = session.execute(String.format("select max(o_id) as o_id, sum(o_ol_cnt) as o_ol_cnt, sum(total_ol_quantity) as total_ol_quantity from order_table where o_w_id = %d and o_d_id = %d;",i,j)).one();
                    count = count.add(row.getBigDecimal("o_ol_cnt"));
                    quantity+=row.getInt("total_ol_quantity");
                    maxOid = Math.max(maxOid, row.getInt("o_id"));
                }
            }
            for(int a=1;a<=10;a++) {
                Row row = session.execute(String.format("select sum(d_next_o_id) as d_next_o_id from order_table where o_w_id = 1 and o_d_id = %d limit 1;", a)).one();
                nextOid +=row.getInt("d_next_o_id");
            }

            long s_quantity = 0;
            BigDecimal s_ytd = new BigDecimal(0);
            long order_cnt = 0;
            long remote_cnt = 0;

            String sum_of_stock = "select sum(S_QUANTITY) as \"sum(s_quantity)\" , sum(S_YTD) as \"sum(s_ytd)\", sum(S_ORDER_CNT) as \"sum(s_order_cnt)\" , sum(S_REMOTE_CNT) as \"sum(s_remote_cnt)\" from stock WHERE S_W_ID = ?; ";
            PreparedStatement stockSum = session.prepare(sum_of_stock);

            for (int i= 1; i <=10 ; i++){
            ResultSet res = session.execute(stockSum.bind(i));
            Row result = res.one();
            s_quantity = s_quantity + result.getInt("sum(s_quantity)");
            s_ytd = s_ytd.add(result.getBigDecimal("sum(s_ytd)"));
            order_cnt = order_cnt + result.getInt("sum(s_order_cnt)");
            remote_cnt = remote_cnt + result.getInt("sum(s_remote_cnt)");
            }

            String dbStateString = new StringBuilder()
                .append(w_ytd.toString()).append(System.lineSeparator())
                .append(d_ytd_sum.toString()).append(System.lineSeparator())
                .append(nextOid).append(System.lineSeparator())
                .append(c_balance_sum.toString()).append(System.lineSeparator())
                .append(c_ytd_payment_sum.toString()).append(System.lineSeparator())
                .append(c_payment_cnt_sum).append(System.lineSeparator())
                .append(c_delivery_cnt_sum).append(System.lineSeparator())
                .append(maxOid).append(System.lineSeparator())
                .append(count.toString()).append(System.lineSeparator())
                .append(ol_amount_sum.toString()).append(System.lineSeparator())
                .append(quantity).append(System.lineSeparator())
                .append(s_quantity).append(System.lineSeparator())
                .append(s_ytd.toString()).append(System.lineSeparator())
                .append(order_cnt).append(System.lineSeparator())
                .append(remote_cnt).append(System.lineSeparator())
                .toString();

            try(PrintWriter dbStateWriter = new PrintWriter(new FileWriter(new File(Constants.METRICS_DIR, "dbState.csv")))) {
                dbStateWriter.println(dbStateString);
            } catch (IOException ignore) {
                
            }
        }
    }
}

