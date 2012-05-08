package kinra.test.hbase.basicclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;

public class MoveRegionTest_92 {
	private static HBaseAdmin admin = null;
	private static ClusterStatus clusterStatus = null;
	private static Configuration configuration = null;
	private static HTablePool hTablePool = null;

	public static void main(String[] args) throws IOException {
		/*
		 * Get all regions servers and their respective assignments Need to get
		 * a random region. Then to find time it was unavailable -> Either
		 * connect to this region, some kind of row lock or something -> Or if
		 * it is a synchronous call we can calculate the time here Then select a
		 * region server other than the one it is hosted on. -> Randomly -> Some
		 * plan Then move the client and repeat till the user specified.
		 */
		initConfig();
		NavigableMap<HRegionInfo, ServerName> allTableAllRegions = new TreeMap<HRegionInfo, ServerName>();
		int numberOfRandomRegionsToMove = 10;
		List<ServerName> allLiveRegionServers = new ArrayList<ServerName>();

		HTableDescriptor[] allUserTables = admin.listTables("testTable");
		hTablePool = new HTablePool(configuration, 10);
		for (HTableDescriptor tableDesc : allUserTables) {
			HTable htable = (HTable)hTablePool.getTable(tableDesc.getName());

			Map<HRegionInfo, ServerName> mapRegionsThisTable = htable
					.getRegionLocations();
			System.out.println("mapRegionsThisTable = " + mapRegionsThisTable);

			for (Map.Entry<HRegionInfo, ServerName> serverEntry : mapRegionsThisTable
					.entrySet()) {

				ServerName sTemp = serverEntry.getValue();
				System.out.println("sTemp = " + sTemp);
				System.out.println("serverEntry.getKey()= "
						+ serverEntry.getKey());
				allTableAllRegions.put(serverEntry.getKey(), sTemp);
			}

		}

		allLiveRegionServers.addAll(clusterStatus.getServers());
		// HServerInfo s = null;

		// Now we have all the table Regions. We just need a list of all the
		// servers
		int i = 10;
		long totalTimeTaken = 0;
		for (Map.Entry<HRegionInfo, ServerName> regionServerEntry : allTableAllRegions
				.entrySet()) {
			totalTimeTaken += moveRegion(regionServerEntry.getKey(),
					regionServerEntry.getValue(), allLiveRegionServers);
			if (i++ >= numberOfRandomRegionsToMove) {
				break;
			}
		}

		System.out
				.println(String
						.format("totalTimeTaken to move all the regions = %s , Average time taken to move a region = %s",
								totalTimeTaken, totalTimeTaken
										/ numberOfRandomRegionsToMove));
	}

	private static long moveRegion(HRegionInfo regionToMove,
			ServerName currentServer, List<ServerName> allLiveRegionServers)
			throws UnknownRegionException, MasterNotRunningException,
			ZooKeeperConnectionException {
		System.out.println("allLiveRegionServers = " + allLiveRegionServers);
		System.out.println("allLiveRegionServers size= "
				+ allLiveRegionServers.size());
		System.out.println("currentServer = " + currentServer);
		if (allLiveRegionServers.size() <= 1) {
			System.out.println("Only one server, hence no movement required");
			return 0;
			//
		}
		ServerName chosenServer = getRandomDestinationServer(
				allLiveRegionServers, currentServer);

		String combinedHServerAddress = chosenServer.getServerName();
		/*
		 * getHostname() + "," + chosenServer.getInfoPort() + "," +
		 * chosenServer.getStartCode();
		 */
		long moveStartTime = System.currentTimeMillis();
		if (!regionToMove.isMetaRegion() && !regionToMove.isRootRegion()) {
			admin.move(regionToMove.getEncodedNameAsBytes(),
					combinedHServerAddress.getBytes());
			System.out
					.println(String
							.format("#####################Would have moved region = %s to address = %s",
									regionToMove.getEncodedName(),
									combinedHServerAddress.getBytes()));
			System.out.println("combinedHServerAddress =  "
					+ combinedHServerAddress);
		} else {
			System.out.println("Cannot move region as it is meta region");
		}

		allLiveRegionServers.add(currentServer);
		return System.currentTimeMillis() - moveStartTime;
	}

	private static ServerName getRandomDestinationServer(
			List<ServerName> allLiveRegionServers, ServerName currentServer) {
		allLiveRegionServers.remove(currentServer);
		System.out.println("allLiveRegionServers.size = "
				+ allLiveRegionServers.size());
		Random r = new Random();
		System.out.println("allLiveRegionServers.length = "
				+ allLiveRegionServers.size());
		int rGet = r.nextInt(allLiveRegionServers.size());
		System.out.println("rGet = " + rGet);
		return allLiveRegionServers.get(rGet);
	}

	private static void initConfig() throws IOException {
		final Configuration temp1 = HBaseConfiguration.create();
		Configuration hConf = HBaseConfiguration.create(temp1);
		hConf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM,
				Constants.hbaseZookeeperQuorum);
		hConf.set(Constants.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT,
				Constants.hbaseZookeeperClientPort);

		// temp1.set("hbase.master", "http://localhost/"); // TODO replace this
		configuration = HBaseConfiguration.create(hConf);
		admin = new HBaseAdmin(configuration);
		clusterStatus = admin.getClusterStatus();

	}
}