package kinra.test.hbase.basicclient;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseClientCreateData {
    private static Configuration configuration = null;
    
    private static void initConfig() throws IOException {
		final Configuration temp1 = HBaseConfiguration.create();
		Configuration hConf = HBaseConfiguration.create(temp1);
		hConf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, Constants.hbaseZookeeperQuorum);
		hConf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, Constants.hbaseZookeeperClientPort);
		
		//temp1.set("hbase.master", "http://localhost/"); // TODO replace this
		configuration = HBaseConfiguration.create(hConf);
		

	}
    
    public static void main(String[] args) throws Exception {
    	initConfig();
        
        HTable htable = new HTable(configuration, "testTable1");
        htable.setAutoFlush(false);
        htable.setWriteBufferSize(1024 * 1024 * 12);

        int totalRecords = 100000;
        int maxID = totalRecords / 1000;
        Random rand = new Random();
        System.out.println("importing " + totalRecords + " records ....");
        for (int i = 0; i < totalRecords; i++) {
            int userID = rand.nextInt(maxID) + 1;
            byte[] rowkey = Bytes.add(Bytes.toBytes(userID), Bytes.toBytes(i));
            String randomPage = Integer.toString(rand.nextInt() * 1000);
            Put put = new Put(rowkey);
            put.add(Bytes.toBytes("testTableColFam1"), Bytes.toBytes("page"),
                    Bytes.toBytes(randomPage));
            htable.put(put);
        }
        htable.flushCommits();
        htable.close();
        System.out.println("done");
    }
}