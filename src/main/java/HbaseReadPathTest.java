package com.bhavani.hbaseclient;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;

public class HbaseReadPathTest {

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

		ResultScanner ss = null; // initializing to to null

		try {
			HTable table = new HTable(conf, "emp");
			Scan s = new Scan();
			ss = table.getScanner(s);
			for (Result r : ss) {
				for (KeyValue kv : r.raw()) {
					System.out.print(new String(kv.getRow()) + " ");// printing row
					System.out.print(new String(kv.getFamily()) + ":");// printing column
					System.out.print(new String(kv.getQualifier()) + " "); // priniting Qualifier
					System.out.print(kv.getTimestamp() + " "); // getting kv timestamp
					System.out.println(new String(kv.getValue())); // getting the value
				}

			}

		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {

			ss.close();
		}
	}

}
