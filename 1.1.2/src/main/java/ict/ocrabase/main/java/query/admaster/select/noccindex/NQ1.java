package ict.ocrabase.main.java.query.admaster.select.noccindex;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * select count(*) as events, count(distinct distinctID) as users from event where data between '2016-07-01' and '2016-07-30'
 * @author houliang
 *
 */
public class NQ1 {
	private static Configuration conf;
	
	public static void queryByColumnRange(String startDate, String endDate,
			String tableName, int scanCache, int threads,String saveFile) throws IOException {
		File datasource = new File(saveFile);
		FileWriter fileWriter;
		try {
			datasource.createNewFile();
			fileWriter = new FileWriter(datasource, true);
		} catch (IOException e) {
			System.err.println("create file failed");
			e.printStackTrace();
		}
		
		conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tableName);
		List<Filter> filters = new ArrayList<Filter>();

		Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("f"),
				Bytes.toBytes("c35"), CompareOp.GREATER,
				Bytes.toBytes(startDate));
		filters.add(filter1);

		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("f"),
				Bytes.toBytes("c35"), CompareOp.LESS, Bytes.toBytes(endDate));
		filters.add(filter2);

		Scan s = new Scan();
		s.setCaching(scanCache);
		FilterList filterList1 = new FilterList(filters);

		s.setFilter(filterList1);

		ResultScanner rs = table.getScanner(s);
		long count = 0;
		fileWriter = new FileWriter(datasource);
		
		HashMap<String, String> map = new HashMap<String, String>();
		for(Result r:rs){
			count++;
			String distinctID = new String(r.getValue(
					Bytes.toBytes("f"), Bytes.toBytes("c24")));
			map.put(distinctID, "");
		}
		fileWriter.write("events:"+count+",\t"+"users:"+map.size()+"\n");
		fileWriter.flush();
		fileWriter.close();
		table.close();
	}
	
	
	
	
	


	public static void main(String[] args) throws IOException {
		if (args.length != 6) {
			System.out.println("wrong parameter");
			return;
		}
		String startDate = args[0];
		String endDate = args[1];
		String tableName = args[2];
		int scanCache = Integer.parseInt(args[3]);
		int threads = Integer.parseInt(args[4]);
		String saveFile=args[5];
//		System.out.println(startDate + "," + endDate + ","+saveFile+","
//				+ tableName + "," + scanCache + "," + threads);
		long startTime = System.currentTimeMillis();
		queryByColumnRange(startDate, endDate, tableName, scanCache, threads,saveFile);
		long endTime = System.currentTimeMillis();
		System.out.println("endtime - starttime = " + (endTime - startTime)
				+ " ms");

	}
}
