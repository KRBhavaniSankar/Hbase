package com.bhavani.hbaseclient;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class PutDataIntoHbase {

	public static void main(String[] args) throws IOException, MasterNotRunningException {

		Configuration conf = new HBaseConfiguration().create();
		HBaseAdmin admin = new HBaseAdmin(conf);

		HTable hTable = new HTable(conf, "Bhavani");
		/*
		 * Put p = new Put(Bytes.toBytes("row1")); p.add(Bytes.toBytes("Personal"),
		 * Bytes.toBytes("name"), Bytes.toBytes("Bhavanisankar"));
		 * 
		 * p.add(Bytes.toBytes("Personal"), Bytes.toBytes("city"),
		 * Bytes.toBytes("delhi")); p.add(Bytes.toBytes("Personal"),
		 * Bytes.toBytes("designation"), Bytes.toBytes("manager"));
		 * p.add(Bytes.toBytes("Professional"), Bytes.toBytes("salary"),
		 * Bytes.toBytes("50000")); p.add(Bytes.toBytes("Professional"),
		 * Bytes.toBytes("desination"), Bytes.toBytes("datascience"));
		 * 
		 * hTable.put(p);
		 */
		/*
		 * Get get = new Get(Bytes.toBytes("row1"));
		 * get.addFamily(Bytes.toBytes("Personal"));
		 * 
		 * Result res = hTable.get(get);
		 * 
		 * byte[] value1 = res.getValue(Bytes.toBytes("Personal"),
		 * Bytes.toBytes("name")); byte[] value2 =
		 * res.getValue(Bytes.toBytes("Personal"), Bytes.toBytes("city")); // byte[]
		 * value3 = res.getValue(Bytes.toBytes("Professional"), //
		 * Bytes.toBytes("salary"));
		 * 
		 * String name = Bytes.toString(value1); String city = Bytes.toString(value2);
		 * // String sal = value3.toString(); System.out.println("name" + name +
		 * "\t city " + city + "\t salary");
		 * 
		 * 
		 */

		Scan scan = new Scan();

		scan.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("name"));
		scan.addColumn(Bytes.toBytes("Personal"), Bytes.toBytes("city"));
		scan.addColumn(Bytes.toBytes("Professional"), Bytes.toBytes("salary"));
		scan.addColumn(Bytes.toBytes("Professional"), Bytes.toBytes("designation"));

		ResultScanner rs = hTable.getScanner(scan);
		for (Result result = rs.next(); result != null; result = rs.next()) {
			System.out.println("\nFound ROW" + result+"\n");
		}
		rs.close();
		System.out.println("succesfully exported !!");
		hTable.close();
	}

}
