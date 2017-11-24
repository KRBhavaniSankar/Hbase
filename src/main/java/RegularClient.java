package com.bhavani.hbaseclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class RegularClient {

	private static Configuration hbaseBconf = null;
	/**
	 * Initialization the Hbase config file as static so that it remain in memory
	 * during the lifecycel of the code executions or till the time the server is is
	 * not recycled or shutdown.
	 */
	static {
		hbaseBconf = HBaseConfiguration.create();
	}

	public static void main(String[] args) {

		try {
			String myHbaseBtableName = "mywebproject:mybrowsedata";
			String[] cfamlies = { "web", "websitedata" };
			RegularClient.creatTable(myHbaseBtableName, cfamlies);
			// add record www.google.com by calling addRecord method
			RegularClient.addRecord(myHbaseBtableName, "www.google.com", "web", "", "best");
			RegularClient.addRecord(myHbaseBtableName, "www.google.com", "websitedata", "", "search");
			RegularClient.addRecord(myHbaseBtableName, "www.google.com", "websitedata", "gmail", "email");
			RegularClient.addRecord(myHbaseBtableName, "www.google.com", "websitedata", "google+", "hangout");
			// add record www.yahoo.com
			RegularClient.addRecord(myHbaseBtableName, "www.yahoo.com", "web", "", "secondbest");
			RegularClient.addRecord(myHbaseBtableName, "www.yahoo.com", "websitedata", "email", "good email system");
			System.out.println("||===========get www.google.com record========||");
			RegularClient.getOneRecord(myHbaseBtableName, "www.google.com");
			System.out.println("||===========show all record========||");
			RegularClient.getAllRecord(myHbaseBtableName);
			System.out.println("||===========del www.yahoo.com record========||");
			RegularClient.delRecord(myHbaseBtableName, "www.yahoo.com");
			RegularClient.getAllRecord(myHbaseBtableName);
			System.out.println("||===========show all record========||");
			RegularClient.getAllRecord(myHbaseBtableName);
		} catch (Exception ep) {
			ep.printStackTrace();
		}
	}

	private static void addRecord(String myHbaseBtableName, String hbaseBrowKey, String hbaseCfamily,
			String hbaseBqualifier, String hbaseBvalue) {
		// TODO Auto-generated method stub
		HTable hbaseBtable = null;
		try {
			hbaseBtable = new HTable(hbaseBconf, myHbaseBtableName); // adding the hbaseBtable name
			Put put = new Put(Bytes.toBytes(hbaseBrowKey));// PUT hbaseBrowKey in bytes
			put.add(Bytes.toBytes(hbaseCfamily), Bytes.toBytes(hbaseBqualifier), Bytes.toBytes(hbaseBvalue));
			// adding column hbaseCfamily,qualifire and values in bytes
			hbaseBtable.put(put);// PUT the above created objects to the HTable object
			System.out.println(
					"Recored  inserted" + hbaseBrowKey + " to hbaseBtable the table: " + myHbaseBtableName + " Done.");
			// showing the inserted records
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				hbaseBtable.close(); // closing the connections in finally
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Delete a row from an existing table
	 * 
	 * @method delRecord
	 * @inputParameters table Name used, hbaseBrowKey
	 * @return type: no return type as its a void method
	 * 
	 **/

	private static void delRecord(String myHbaseBtableName, String hbaseBrowKey) {
		// TODO Auto-generated method stub
		@SuppressWarnings("resource")
		HTable hbaseBtable = null;
		try {
			hbaseBtable = new HTable(hbaseBconf, myHbaseBtableName);
			List<Delete> mylist = new ArrayList<Delete>();
			Delete delKey = new Delete(hbaseBrowKey.getBytes());
			mylist.add(delKey);
			hbaseBtable.delete(mylist);
		} catch (IOException ee) {
			//
			ee.printStackTrace();
		} finally {
			try {
				hbaseBtable.close(); // closing the connections in finally
			} catch (IOException ep) {
				ep.printStackTrace();
			}
		}
		System.out.println("delete Recored " + hbaseBrowKey + " ok.");

	}

	/**
	 * Getting all records a row from an existing SS tables
	 * 
	 * @method getAllRecord
	 * @inputParameters hbaseBtable Name used
	 * @return type: no return type as its a void method
	 * 
	 **/
	@SuppressWarnings({ "deprecation", "resource" })

	private static void getAllRecord(String myHbaseBtableName) {
		// TODO Auto-generated method stub
		ResultScanner hbaseBSs = null;
		try {
			HTable hbaseBtable = new HTable(hbaseBconf, myHbaseBtableName);
			Scan hbaseBScan = new Scan();
			hbaseBSs = hbaseBtable.getScanner(hbaseBScan);
			for (Result r : hbaseBSs) {
				for (KeyValue hbaseBkv : r.raw()) {
					System.out.print(new String(hbaseBkv.getRow()) + " ");
					System.out.print(new String(hbaseBkv.getFamily()) + ":");
					System.out.print(new String(hbaseBkv.getQualifier()) + " ");
					System.out.print(hbaseBkv.getTimestamp() + " ");
					System.out.println(new String(hbaseBkv.getValue()));
				}
			}
		} catch (IOException eio) {
			eio.printStackTrace();
		} finally {
			if (hbaseBSs != null)
				hbaseBSs.close();
			// closing the ss hbaseBtable
		}

	}

	/**
	 * Getting a row from an existing table
	 * 
	 * @method getOneRecord
	 * @inputParameters table Name used, hbaseBrowKey
	 * @return type: no return type as its a void method
	 * 
	 **/
	@SuppressWarnings("deprecation")
	private static void getOneRecord(String myHbaseBtableName, String hbaseBrowKey) {

		// TODO Auto-generated method stub

		HTable hbaseBtable;
		Result hbaseBrs = null;
		try {
			hbaseBtable = new HTable(hbaseBconf, myHbaseBtableName);
			Get hbaseBget = new Get(hbaseBrowKey.getBytes());
			hbaseBrs = hbaseBtable.get(hbaseBget);
			for (KeyValue hbaseBkv : hbaseBrs.raw()) {
				System.out.print(new String(hbaseBkv.getRow()) + " ");
				System.out.print(new String(hbaseBkv.getFamily()) + ":");
				System.out.print(new String(hbaseBkv.getQualifier()) + " ");
				System.out.print(hbaseBkv.getTimestamp() + " ");
				System.out.println(new String(hbaseBkv.getValue()));
			}
		} catch (IOException eio) {
			eio.printStackTrace();
		} finally {
			if (!hbaseBrs.isEmpty())
				hbaseBrs = null; // forcing the resultset to clear the hbaseBrs object in finally
			// Change this method accrodingly if you want to use it in your code
			// if you want to return then pls change accordingly.
		}

	}

	/**
	 * Create a hbaseBtable in case the table is not there Else prints table already
	 * exist conf is the configuration file object which needs to be passed,
	 * hbase-site.xml
	 * 
	 * @method creatTable
	 * @inputParameters: tablename as String, ColumnFamalies as String Array[]
	 * @return NA as is a voild method
	 **/

	private static void creatTable(String myHbaseBtableName, String[] cfamlies) throws Exception {
		// TODO Auto-generated method stub

		HBaseAdmin hbaseBadmin = new HBaseAdmin(hbaseBconf);
		if (hbaseBadmin.tableExists(myHbaseBtableName)) {
			System.out.println("Ohh.. this table is already there");
		} else {
			@SuppressWarnings("deprecation")
			HTableDescriptor hbaseBtableDesc = new HTableDescriptor(myHbaseBtableName);
			for (int i = 0; i < cfamlies.length; i++) {
				hbaseBtableDesc.addFamily(new HColumnDescriptor(cfamlies[i]));
			}
			hbaseBadmin.createTable(hbaseBtableDesc);// creating a table
			System.out.println("create table " + myHbaseBtableName + " Done.");
			// Table is created and printed with the name.
		}

	}

}