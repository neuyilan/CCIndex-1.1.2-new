package ict.ocrabase.main.java.query.admaster.select;

import ict.ocrabase.main.java.client.index.IndexNotExistedException;
import ict.ocrabase.main.java.client.index.IndexResultScanner;
import ict.ocrabase.main.java.client.index.IndexTable;
import ict.ocrabase.main.java.client.index.Range;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * select count(*) as events, count(distinct distinctID) as users from event where data between '2016-07-01' and '2016-07-30'
 * @author houliang
 *
 */
public class Q1 {
	
	
	public static void queryTest(String startDate, String endDate,
			String tableName, int scanCache, int threads,String saveFile)
			throws IOException {
		File datasource = new File(saveFile);
		FileWriter fileWriter;
		try {
			datasource.createNewFile();
			fileWriter = new FileWriter(datasource, true);
		} catch (IOException e) {
			System.err.println("create file failed");
			e.printStackTrace();
		}
		
		IndexTable indextable = new IndexTable(tableName);
		indextable.setScannerCaching(scanCache);
		indextable.setMaxScanThreads(threads);
		// System.out.println("max thread:" + indextable.getMaxScanThreads());

		Range[] ranges = new Range[1];
		ranges[0] = new Range(indextable.getTableName(), Bytes.toBytes("f:c35"));
		ranges[0].setStartType(CompareOp.GREATER);
		ranges[0].setStartValue(Bytes.toBytes(startDate));
		ranges[0].setEndType(CompareOp.LESS);
		ranges[0].setEndValue(Bytes.toBytes(endDate));


		byte[][] resultcolumn = new byte[1][];
		resultcolumn[0] = Bytes.toBytes("f:c24");
		try {
			IndexResultScanner rs = indextable.getScanner(
					new Range[][] { ranges }, resultcolumn);

			Result r;

//			Map<byte[], DataType> columnMap = indextable.getColumnInfoMap();

			long count = 0;
			fileWriter = new FileWriter(datasource);
			HashMap<String, String> map = new HashMap<String, String>();
			while ((r = rs.next()) != null) {
				count++;
				String distinctID = new String(r.getValue(
						Bytes.toBytes("f"), Bytes.toBytes("c24")));
				map.put(distinctID, "");
			}
			fileWriter.write("events:"+count+",\t"+"users:"+map.size()+"\n");
			fileWriter.flush();
			fileWriter.close();
		} catch (IndexNotExistedException e) {
			System.err.println("error query");
			e.printStackTrace();
		}

	}
	
//	public static void write2file(String str,String saveFile){
//		try {
//			FileWriter fileWriter = new FileWriter(datasource, true);
//			fileWriter.write(str);
//			fileWriter.flush();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}

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
		queryTest(startDate, endDate, tableName, scanCache, threads,saveFile);
		long endTime = System.currentTimeMillis();
		System.out.println("endtime - starttime = " + (endTime - startTime)
				+ " ms");

	}
}
