package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest3 {

	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", "192.168.56.101"); 
		config.set("hbase.zookeeper.quorum", "192.168.56.101");
//		config.set("hbase.zookeeper.property.clientPort","2182");

        HTable testTable = new HTable(config, "mytable");
        
        for (int i = 0; i < 100; i++) {
            byte[] family = Bytes.toBytes("cf");
            byte[] qual = Bytes.toBytes("a");

            Scan scan = new Scan();
            scan.addColumn(family, qual);
            ResultScanner rs = testTable.getScanner(scan);
            for (Result r = rs.next(); r != null; r = rs.next()) {
                byte[] valueObj = r.getValue(family, qual);
                String value = new String(valueObj);
                System.out.println(value);
            }
        }
        
        testTable.close();
	}
}
