package com.bhavani.hbaseclient;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseWritePathTest {

	public static void main(String[] args) throws IOException {

		Configuration conf = new HBaseConfiguration().create();

		conf.set("hbase.zookeeper.property.clientPort", "2181");// zookeeprs port on which zookeepr is listening.

		HBaseAdmin admin = new HBaseAdmin(conf); // passing the configuraiton
		try {
			HBaseAdmin.checkHBaseAvailable(conf);
			System.out.println("Test for success ! ");
		} catch (Exception error) {
			System.err.println("Error connecting HBase:  " + error);
			System.exit(1);
		}

		HTable hTable = new HTable(conf, "emp");
		Put p = new Put(Bytes.toBytes("4"));
		p.add(Bytes.toBytes("personaldata"), Bytes.toBytes("name"), Bytes.toBytes("Bhavani"));

		p.add(Bytes.toBytes("personaldata"), Bytes.toBytes("city"), Bytes.toBytes("bengaluru"));
		p.add(Bytes.toBytes("personaldata"), Bytes.toBytes("designation"), Bytes.toBytes("programmer"));
		p.add(Bytes.toBytes("professionaldata"), Bytes.toBytes("salary"), Bytes.toBytes("50000"));
		p.add(Bytes.toBytes("professionaldata"), Bytes.toBytes("desination"), Bytes.toBytes("dataengineer"));

		hTable.put(p);

		System.out.println("Succesfully exported data into Hbase table!!");

	}

}
