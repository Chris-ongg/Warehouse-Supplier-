package wholesale;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static wholesale.Constants.KEYSPACE_NAME;

public class Main {

    public static void main(String[] args) {
        try (CqlSession session = CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                    .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM")
                    .build())
                .build();) {
            session.execute(String.format("USE %s", KEYSPACE_NAME));

            int serverNum = Integer.parseInt(args[0]);
            
            Map<Integer, Future<Measurement>> futureMeasurements = new HashMap<>();
            ExecutorService executor = Executors.newFixedThreadPool(8);

            for(int i=((serverNum-1) * 8);i< (serverNum*8);i++){
                Future<Measurement> future = executor.submit(new ClientThread(i, session));
                futureMeasurements.put(i, future);
            }

            double minThroughput = Double.MAX_VALUE;
            double maxThroughput = Double.MIN_VALUE;
            double totalThroughput = 0;

            StringBuilder clientMetricsSBuilder = new StringBuilder();

            for (int client: futureMeasurements.keySet()) {
                Future<Measurement> future = futureMeasurements.get(client);
                String [] performanceData = createPerformanceData(future);

                if (performanceData[0] == null || performanceData[0].isEmpty()) {
                    Util.debugLog("Main", "No metrics from client " + client);
                } else {
                    clientMetricsSBuilder.append(performanceData[0]).append(System.lineSeparator());
                }

                if (performanceData[1] == null || performanceData[1].isEmpty()) {
                    Util.debugLog("Main", "No throughput metrics from client " + client);
                } else {
                    double throughput = Double.parseDouble(performanceData[1]);
                    minThroughput = Math.min(minThroughput, throughput);
                    maxThroughput = Math.max(maxThroughput, throughput);      
                    totalThroughput += throughput;
                }
            }

            double avgThroughPut = totalThroughput/8;

            String clientFile = "clients_" + serverNum + ".csv";
            String throughputFile = "throughput_" + serverNum + ".csv";

            String clientMetrics = clientMetricsSBuilder.toString();
            String throughputMetrics = String.join(",", String.valueOf(minThroughput), 
                                    String.valueOf(maxThroughput), String.valueOf(avgThroughPut));

            try(PrintWriter clientFileWriter = new PrintWriter(new FileWriter(new File(Constants.METRICS_DIR, clientFile)));
                PrintWriter throughputWriter = new PrintWriter(new FileWriter(new File(Constants.METRICS_DIR, throughputFile)))) {
                throughputWriter.println(throughputMetrics);
                clientFileWriter.println(clientMetrics);
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println(String.format("Throughput of %d: %s%sClient Metrics of %d: %s", serverNum, 
                                        throughputMetrics, System.lineSeparator(), serverNum, clientMetrics));
            }
            executor.shutdown();
        }
    }
    public static String[] createPerformanceData(Future<Measurement> future) {
        StringBuilder record = new StringBuilder();
        String [] performanceData = new String[2];
        Measurement m;
        try {
            m = future.get();
            double totalLatency = 0;
            for(long latency : m.transactionLatency) {
                totalLatency += latency;
            }
            double throughput = totalLatency > 0 ? (double) m.transactionCount / totalLatency : 0;
            double totalLatencyInSecs = totalLatency / 1000;
            double avgLatency = m.transactionCount > 0 ? totalLatencyInSecs/ (double) m.transactionCount : 0;
            
            Collections.sort(m.transactionLatency);
            int latencyLen = m.transactionLatency.size();
          
	        double medianLatency = 0;
            long percentile_95th = 0;
            long percentile_99th = 0;
            if (m.transactionLatency.size() > 0) {
                if (latencyLen % 2 == 0) {
                    medianLatency = ((double)m.transactionLatency.get(latencyLen/2) + (double)m.transactionLatency.get(latencyLen/2 -1))/2;
                } else {
                    medianLatency = (double)m.transactionLatency.get(latencyLen/2);
                }
                percentile_95th = percentile(m.transactionLatency, 95);
                percentile_99th = percentile(m.transactionLatency, 99);
            }
           
            record.append(m.index + ", ");
            record.append(m.transactionCount + ", ");
            record.append(BigDecimal.valueOf(totalLatencyInSecs).setScale(2, RoundingMode.HALF_UP).toPlainString() + ", ");
            record.append(BigDecimal.valueOf(throughput).setScale(2, RoundingMode.HALF_UP).toPlainString() + ", ");
            record.append(BigDecimal.valueOf(avgLatency).setScale(2, RoundingMode.HALF_UP).toPlainString() + ", ");
            record.append(BigDecimal.valueOf(medianLatency).setScale(2, RoundingMode.HALF_UP).toPlainString() + ", ");
            record.append(percentile_95th + ", ");
            record.append(percentile_99th );

            performanceData[0] = record.toString();
            performanceData[1] = Double.toString(throughput);
            System.err.println(performanceData[0]);

        } catch(InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return performanceData;
    }
    public static long percentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index-1);
    }
}
