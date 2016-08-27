package hbase;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Service;

/**
 * Demonstrating how to access HBase using Spring Data for Hadoop.
 * 
 * @author swang
 * 
 */
@Service
public class HBaseService {

	@Autowired
	private Configuration hbaseConfiguration;

	@Inject
	private HbaseTemplate hbTemplate;

	// Table info
	final String tableName = "report";
	final String columnFamilyData = "data";
	final String colFile = "file";
	final String rowNamePattern = "row";
	final String value = "report24.csv-";

	/**
	 * 
	 * @throws Exception
	 */

	public void run() throws Exception {
		// 1. create table
		createTable();
		// 2. add data entry
		addData();
	}

	/**
	 * Creates HBase table
	 * 
	 * @throws Exception
	 */
	public void createTable() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(hbaseConfiguration);

		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}

		HTableDescriptor tableDes = new HTableDescriptor(tableName);
		HColumnDescriptor cf1 = new HColumnDescriptor(columnFamilyData);
		tableDes.addFamily(cf1);
		admin.createTable(tableDes);
	}

	/**
	 * Adds data entry for report.
	 */
	private void addData() {
		hbTemplate.execute(tableName, new TableCallback<Boolean>() {

			public Boolean doInTable(HTableInterface table) throws Throwable {
				for (int i = 0; i < 1000; i++) {
					Put p = new Put(Bytes.toBytes(rowNamePattern + i));
					p.add(Bytes.toBytes(columnFamilyData), Bytes.toBytes(colFile), Bytes.toBytes(value + i));
					table.put(p);
				}
				return new Boolean(true);
			}
		});
	}

	public static void main(String[] args) throws Exception {
		AbstractApplicationContext ctx = new ClassPathXmlApplicationContext("SpringBeans.xml");

		HBaseService hBaseService = (HBaseService) ctx.getBean("hBaseService");
		hBaseService.run();
	}
}
