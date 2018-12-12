package com.example.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@SpringBootApplication
@RestController
public class DemoApplication {

    public static void main(String[] args) {


        System.setProperty("hadoop.home.dir","E:\\Gitee\\hadoop-common-2.6.0-bin");
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public HbaseTemplate hbaseTemplate(@Value("${hbase.zookeeper.quorum}") String quorum,
                                       @Value("${hbase.zookeeper.port}") String port) {
        HbaseTemplate hbaseTemplate = new HbaseTemplate();
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.port", port);
        hbaseTemplate.setConfiguration(conf);
        hbaseTemplate.setAutoFlush(true);
        return hbaseTemplate;
    }

    public static void createData(byte[] rowKey, byte[] family, byte[] colum, byte[] value) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, "test1");
        TableName tn = TableName.valueOf("myTable");
        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(tn)) {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tn);
            tableDescriptor.addFamily(new HColumnDescriptor(family));
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(tn)) {
                admin.createTable(tableDescriptor);
            }
            Put put = new Put(rowKey);
            put.addColumn(family, colum, value);
            table.put(put);
        }
    }

    @GetMapping("/create")
    public String aa() throws IOException {
        createData("userId-1".getBytes(), "routeId-1".getBytes(), "drivingSpeed".getBytes(), "500km/h".getBytes());
        return "success";
    }

}

