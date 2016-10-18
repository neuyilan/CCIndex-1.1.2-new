package ict.ocrabase.main.java.query.useCC.orQuery;

import ict.ocrabase.main.java.client.index.IndexNotExistedException;
import ict.ocrabase.main.java.client.index.IndexResultScanner;
import ict.ocrabase.main.java.client.index.IndexTable;
import ict.ocrabase.main.java.client.index.Range;
import ict.ocrabase.main.java.regionserver.DataType;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.THEAD;
/**
 * select orderkey, orderdate, shippriority from orders where custkey=? or orderdate < ? and orderdate > ?
 * @author houliang
 * c1=12919954	c4=1992-03-15
 */
public class OrderQ3AndOr {
	
	
	public static String println_test(Result result) {
		StringBuilder sb = new StringBuilder();
		sb.append("row=" + Bytes.toString(result.getRow()));

		List<KeyValue> kv = result.getColumn(Bytes.toBytes("f"),
				Bytes.toBytes("c1"));
		if (kv.size() != 0) {
			sb.append(", f:c1=" + Bytes.toString(kv.get(0).getValue()));
		}

		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c2"));
		if (kv.size() != 0) {
			sb.append(", f:c2=" + Bytes.toString(kv.get(0).getValue()));
		}

		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c3"));
		if (kv.size() != 0) {
			sb.append(", f:c3=" + Bytes.toString(kv.get(0).getValue()));
		}

		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c4"));
		if (kv.size() != 0) {
			sb.append(", f:c4=" + Bytes.toString(kv.get(0).getValue()));
		}
		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c5"));
		if (kv.size() != 0) {
			sb.append(", f:c5=" + Bytes.toString(kv.get(0).getValue()));
		}

		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c6"));
		if (kv.size() != 0) {
			sb.append(", f:c6=" + Bytes.toString(kv.get(0).getValue()));
		}
		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c7"));
		if (kv.size() != 0) {
			sb.append(", f:c7=" + Bytes.toString(kv.get(0).getValue()));
		}
		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c8"));
		if (kv.size() != 0) {
			sb.append(", f:c8=" + Bytes.toString(kv.get(0).getValue()));
		}
		return sb.toString();
	}

	public static void queryTest(String custkey, String startOrderDate,String endOrderDate,
			String saveFile, String tableName, int scanCache, int threads)
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

		// the filter for orderdate  
		// where orderdate>'startOrderDate' and orderdate < 'endOrderDate';
		Range[] range1 = new Range[1];
		range1[0] = new Range(indextable.getTableName(), Bytes.toBytes("f:c4"));
		range1[0].setStartType(CompareOp.GREATER);
		range1[0].setStartValue(Bytes.toBytes(startOrderDate));
		range1[0].setEndType(CompareOp.LESS);
		range1[0].setEndValue(Bytes.toBytes(endOrderDate));
		
		//the above is AND operation
		

		//the filter for custkey
//		where custkey='custkey'
		Range[] range2 = new Range[1];
		range2[0] = new Range(indextable.getTableName(), Bytes.toBytes("f:c1"));
		range2[0].setStartType(CompareOp.EQUAL);
		range2[0].setStartValue(Bytes.toBytes(custkey));
		
		// range1 OR range2 opeartion,In other words, where Range[][] ranges, the first dimension is OR operation, 
		//while the second dimension is AND opeartion.
		Range[][] ranges = new Range[2][];
		ranges[0]=range1;
		ranges[1]=range2;
		
		
		
		byte[][] resultcolumn = new byte[3][];
		resultcolumn[0] = Bytes.toBytes("f:c1");
		resultcolumn[1] = Bytes.toBytes("f:c4");
		resultcolumn[2] = Bytes.toBytes("f:c5");
		
		try {
			IndexResultScanner rs = indextable.getScanner(ranges, resultcolumn);

			Result r;

			Map<byte[], DataType> columnMap = indextable.getColumnInfoMap();

			// System.out.println(rs.getTotalScannerNum() +
			// "   "+rs.getTotalCount() +"  "+rs.getFinishedScannerNum());
			long count = 0;
			fileWriter = new FileWriter(datasource);
			while ((r = rs.next()) != null) {
				count++;
				fileWriter.write(println_test(r)+"\n");
				fileWriter.flush();
			}
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
		if (args.length != 7) {
			System.out.println("wrong parameter");
			return;
		}
		String custkey = args[0];
		String startOrderDate = args[1];
		String endOrderDate = args[2];
		String saveFile = args[3];
		String tableName = args[4];
		int scanCache = Integer.parseInt(args[5]);
		int threads = Integer.parseInt(args[6]);
		System.out.println(custkey + "," + startOrderDate + "," +endOrderDate+"," + saveFile + ","
				+ tableName + "," + scanCache + "," + threads);
		long startTime = System.currentTimeMillis();
		queryTest(custkey, startOrderDate, endOrderDate,saveFile, tableName, scanCache, threads);
		long endTime = System.currentTimeMillis();
		System.out.println("endtime - starttime = " + (endTime - startTime)
				+ " ms");

	}
	
//	12919954 1993-03-15 1993-04-15  /home/qhl/ccindex/test-result/orders_out/or_q3  real_table_with_index 1000 10
	
	
}
