package com.bhavani.hbaseclient;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;


public class CreatingTable {

	public static void main(String[] args) throws IOException {

		Configuration conf = new HBaseConfiguration().create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf("Bhavani"));
		//HColumnDescriptor cf = new HColumnDescriptor("personal");
		table.addFamily(new HColumnDescriptor("Personal"));
		table.addFamily(new HColumnDescriptor("Professional"));
		admin.createTable(table);
		System.out.println("Table created Succesfully !!");

	}

}
