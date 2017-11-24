package com.bhavani.hbaseclient;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;


public class ListingTables {

	public static void main(String[] args) throws IOException , MasterNotRunningException{

		Configuration conf = new HBaseConfiguration().create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor[] tabledescriptor = admin.listTables();
		
		for(int i =0 ; i <tabledescriptor.length ; i++)
		{
			System.out.println("\t"+tabledescriptor[i].getNameAsString());
		}
	}

}
