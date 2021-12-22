package wholesale;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Map.Entry;

public class TransactionHandler {

    String logClassName;
    String filePath;
    CqlSession session;

    int executedTransactionCount = 0;
    int totalExecutionTimeInSecs = 0;
    List<Long> transactionLatency = new ArrayList<>();
    NewOrderTransaction newOrder;
    PaymentTransactionHandler payment;
    DeliveryTransactionHandler delivery;
    OrderStatusTransaction orderStatus;
    StockLevelTransaction stockLevel;
    PopularItemsTransaction popularItem;
    TopBalanceTransactionHandler topBalance;
    RelatedCustomerTransaction relatedCustomer;

    int warehouseId, districtId, customerId;

    public TransactionHandler(CqlSession session, int index, String filePath) {
        this.logClassName = TransactionHandler.class.getSimpleName() + "/" + index;
        this.session = session;
        this.filePath = filePath;
        this.newOrder = new NewOrderTransaction(this.session);
        this.payment = new PaymentTransactionHandler(session);
        this.delivery = new DeliveryTransactionHandler(session);
        this.orderStatus = new OrderStatusTransaction(this.session);
        this.stockLevel = new StockLevelTransaction(session);
        this.popularItem = new PopularItemsTransaction(session);
        this.topBalance = new TopBalanceTransactionHandler(session);
        this.relatedCustomer = new RelatedCustomerTransaction(session);
    }

    public Measurement processTransactions() {
        Util.debugLog(logClassName, "Started processing");
        int transactionCount = 0;
        Random r = new Random();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            for (String line; (line = br.readLine()) != null;) {
                String[] values = line.split(",");
                boolean transactionResult = true;
                int retry = 2;
                long st = System.currentTimeMillis();
                switch(values[0]) {
                    case "N":
                        int itemCount = Integer.parseInt(values[values.length - 1]);
                        List<InputOrderItem> orderItems = new ArrayList<>();
                        for(int i=0;i<itemCount;i++) {
                            String [] orderItemsString = br.readLine().split(",");
                            InputOrderItem item = new InputOrderItem(Integer.parseInt(orderItemsString[0]),
                                    Integer.parseInt(orderItemsString[1]), Integer.parseInt(orderItemsString[2]));
                            orderItems.add(item);
                        }
                        customerId = Integer.parseInt(values[1]);
                        warehouseId = Integer.parseInt(values[2]);
                        districtId = Integer.parseInt(values[3]);
                        while (retry > 0) {
                            try {
                                transactionResult = newOrder.executeTransaction(customerId, warehouseId, districtId, orderItems);
                            } catch (Exception e) {
                                Util.debugLog(logClassName, String.format("Retrying #%d of type %s. Reason: %s", transactionCount, values[0], e.getMessage()));
                                transactionResult = false;
                            }
                            if (transactionResult) {
                                break;
                            } else {
			                    try {
                                    Thread.sleep((long) r.nextInt(1000) + 1000);
                                } catch (InterruptedException ignore) {
                                
                                }
			                }
                            retry--;
                        }
                        break;
                    case "P":
                        warehouseId = Integer.parseInt(values[1]);
                        districtId = Integer.parseInt(values[2]);
                        customerId = Integer.parseInt(values[3]);
                        BigDecimal paymentAmt = new BigDecimal(values[4]);
                        while (retry > 0) {
                            try {
                                transactionResult = payment.execute(warehouseId, districtId, customerId, paymentAmt);
                            } catch (Exception e) {
                                Util.debugLog(logClassName, String.format("Retrying #%d of type %s. Reason: %s", transactionCount, values[0], e.getMessage()));
                                transactionResult = false;
                            }
                            if (transactionResult) {
                                break;
                            } else {
			                    try {
                                   Thread.sleep((long) r.nextInt(1000) + 1000);
                                } catch (InterruptedException ignore) {
                                
                                }
                            }
                            retry--;
                        }
                        break;
                    case "D":
                        warehouseId = Integer.parseInt(values[1]);
                        int carrier_id = Integer.parseInt(values[2]);
                        while (retry > 0) {
                            try {
                                transactionResult = delivery.execute(warehouseId, carrier_id);
                            }catch (Exception e) {
                                Util.debugLog(logClassName, String.format("Retrying #%d of type %s. Reason: %s", transactionCount, values[0], e.getMessage()));
                                transactionResult = false;
                            }
                            if (transactionResult) {
                                break;
                            } else {
                                try {
                                   Thread.sleep((long) r.nextInt(1000) + 1000);
                                } catch (InterruptedException ignore) {
                                
                                }
                            }
                            retry--;
                        }
                        break;
                    default:
                        while (retry > 0) {
                            transactionResult = true;
                            try {
                                switch (values[0]) {
                                    case "O":
                                        orderStatus.executeTransaction(Integer.parseInt(values[1]), Integer.parseInt(values[2]),
                                                Integer.parseInt(values[3]));
                                        break;
                                    case "S":
                                        stockLevel.executeTransaction(Integer.parseInt(values[1]) , Integer.parseInt(values[2]) ,
                                                Integer.parseInt(values[3]) , Integer.parseInt(values[4]));
                                        break;
                                    case "I":
                                        popularItem.executeTransaction(Integer.parseInt(values[1]) , Integer.parseInt(values[2]) ,
                                                Integer.parseInt(values[3]));
                                        break;
                                    case "T":
                                        topBalance.execute();
                                        break;
                                    case "R":
                                        relatedCustomer.executeTransaction(Integer.parseInt(values[1]) , Integer.parseInt(values[2]) ,
                                                Integer.parseInt(values[3]));
                                        break;
                                    default:
                                        // log error
                                        break;
                                }
                            } catch(AllNodesFailedException e) {
                                Util.debugLog(logClassName, String.format("Retrying #%d of type %s. Reason: AllNodesFailedException", transactionCount, values[0]));
                                Iterator<Entry<Node, List<Throwable>>> iterator = e.getAllErrors().entrySet().iterator();
                                StringBuilder details = new StringBuilder();
                                while (iterator.hasNext()) {
                                    Entry<Node, List<Throwable>> entry = iterator.next();
                                    details.append(entry.getKey()).append(": ").append(entry.getValue());
                                    for (Throwable t: entry.getValue()) {
                                        details.append(t.getMessage()).append(System.lineSeparator());
                                    }
                                    if (iterator.hasNext()) {
                                        details.append(", ");
                                    }
                                }
                                Util.debugLog(logClassName, details.toString());
                                transactionResult = false;
                            } catch(QueryExecutionException e) {
                                String cause = e.getCause() != null ? e.getCause().getMessage() : "";
                                Util.debugLog(logClassName, String.format("Retrying #%d of type %s. Reason QueryExecutionException: %s; cause by", transactionCount, values[0], e.getMessage(), cause));
                                transactionResult = false;
                            } catch(Exception e) {
                                String cause = e.getCause() != null ? e.getCause().getMessage() : "";
                                Util.debugLog(logClassName, String.format("Retrying #%d of type %s. Reason: %s cause by", transactionCount, values[0], e.getMessage(), cause));
                                transactionResult = false;
                            }
                            if (transactionResult) {
                                break;
                            } else {
				                try {
                                    Thread.sleep((long) r.nextInt(1000) + 1000);
                                } catch (InterruptedException ignore) {
                                
                                }
			                }
                            retry--;
                        }
                        break;
                }
                long et = System.currentTimeMillis();
                long executionTime = (et - st);

                if (transactionResult) {
                    executedTransactionCount++;
                    transactionLatency.add(executionTime);
                }
                transactionCount += 1;
                if (transactionCount % 1000 == 0) {
                    Util.debugLog(logClassName, String.format("Processed %d tx", transactionCount));
                }
            }
        } catch (IOException e) {
            System.err.println(e.toString());
        }
        Util.debugLog(logClassName, "Completed");
        return new Measurement(0, executedTransactionCount, transactionLatency);
    }
}
