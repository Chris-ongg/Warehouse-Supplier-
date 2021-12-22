# Wholesale Supplier Benchmarking using Cassandra

This file describes how to configure, setup and the benchmark the application using Cassandra.

### Prerequisites

Download the following binaries:
- [Java 8](https://openjdk.java.net/install/)
<br> Add `export JAVA_HOME=jdk-install-dir` in `.bash_profile`
- [Apache Cassandra 4.0.0](https://archive.apache.org/dist/cassandra/4.0.0/)
- [Apache Maven](https://maven.apache.org/download.cgi)

### Cluster setup
1. Update the following the fields in the `cassandra.yaml` file of each node.
<br>The path for this file `cassandra-install-dir/conf/cassandra.yaml`
    1. `cluster_name`: set to unique name to ensure that no other nodes join the cluster.
    2. `listen_address`: set the value to the IP address of the node.
    3. `enable_materialized_views`: set the value set to `true`.
    4. `seeds`: set the value to a comma-separated list of IP addresses of  2 or 3 the nodes.
        - This field will be present under the `seed_provider` field.
        - Setting this field allows the nodes to contact the other nodes in the cluster.
2. Start cassandra on each node executing the binary at `cassandra-install-dir/bin/cassandra`

3. Run `cassandra-install-dir/bin/nodetool status` to check if all nodes are up and running.
<br>The Status/State for each node should be **UN**.

For more details, refer to https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/initialize/initSingleDS.html

### Database Setup

1. Copy this code repository to each node.
<br> Run `cd cassandra`.

2. Compile the code using `mvn-install-dir/bin/mvn clean dependency:copy-dependencies package`

3. On any one of the nodes, running the following to setup the database.
    1. Run `jdk-install-dir/bin/java -Xms2g -Xmx8g -cp target/*:target/dependency/*:. wholesale.Setup`.
    <br>This will create the required keyspace, tables and user-defined types.
to create key space and schemas used in this project.

    2. Run `cqlsh -f load_data.cql`
    <br>This will load the given data into the database.

### Benchmark Execution

1. Start the benchmark on each node by running `jdk-install-dir/java -Xms2g -Xmx8g -cp target/*:target/dependency/*:. wholesale.Main <node_number>`
<br> On each node pass `node_number`. The value should be between 1 to 5. The values passed on each node should be unique to ensure that they execute distinct transactions files.

2. After the each node completes processing its set of transactions, the client and throughput metrics are saved as csv files in `./metrics` directory. The client metrics is also sent to `stderr`.

3. Once all the nodes have finished executing, to generate the `dbState.csv`, run `jdk-install-dir/bin/java -Xms2g -Xmx8g -cp target/*:target/dependency/*:. wholesale.DbState`. The file will be present at `./metrics` directory.
