package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseTest {

	public static void main(String[] args) throws IOException {
		String tableName = "test2";
        String rowKey = "rowkey";
        String[] cfs = new String[] {"cf1","cf2","cf2"};
        String[] qfs = new String[] {"qualifier1","qualifier2","qualifier3"};
        String[] vals = new String[] {"value1","value2","value3"};
         
        // configuration
        Configuration config = HBaseConfiguration.create();
//        config.clear();
//        config.set("hbase.master", "52.78.20.2:60000"); 
//		config.set("hbase.zookeeper.quorum", "52.78.20.2");
		config.set("hbase.zookeeper.quorum", "10.0.2.15");
		config.set("hbase.zookeeper.property.clientPort","2181");
		
		HBaseAdmin.checkHBaseAvailable(config);
 
        // 테이블 생성 & 컬럼 추가
        HBaseAdmin hbase = null;
        try {
            hbase = new HBaseAdmin(config);
            System.out.println("HBase Connection succeded...");
            HTableDescriptor desc = new HTableDescriptor(tableName);
            for(int i=0; i<cfs.length; i++) {
                HColumnDescriptor meta = new HColumnDescriptor(cfs[i].getBytes());
                desc.addFamily(meta);
            }
            hbase.createTable(desc); // create
        } finally {
            if(hbase !=null) hbase.close();
        }
	}
}
