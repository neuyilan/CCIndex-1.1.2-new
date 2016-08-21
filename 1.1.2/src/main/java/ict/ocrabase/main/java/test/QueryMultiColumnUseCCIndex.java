package ict.ocrabase.main.java.test;

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
//secondary delay average 
//ccindex delay average

public class QueryMultiColumnUseCCIndex {


	public static void println_test(Result result) {
		StringBuilder sb = new StringBuilder();
		sb.append("row=" + Bytes.toString(result.getRow()));

		List<KeyValue> kv = result.getColumn(Bytes.toBytes("f"),
				Bytes.toBytes("c1"));
		if (kv.size() != 0) {
			sb.append(", f:c1=" + Bytes.toInt(kv.get(0).getValue()));
		}

		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c2"));
		if (kv.size() != 0) {
			sb.append(", f:c2=" + Bytes.toString(kv.get(0).getValue()));
		}

		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c3"));
		if (kv.size() != 0) {
			sb.append(", f:c3=" + Bytes.toDouble(kv.get(0).getValue()));
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
			sb.append(", f:c7=" + Bytes.toInt(kv.get(0).getValue()));
		}
		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c8"));
		if (kv.size() != 0) {
			sb.append(", f:c8=" + Bytes.toString(kv.get(0).getValue()));
		}
		System.out.println(sb.toString());
	}

	public static void queryTest() throws IOException {
		File datasource = new File("/opt/qhl/downloaddata/CCIndex/output");
		try {
			datasource.createNewFile();
		} catch (IOException e) {
			System.err.println("create file failed");
			e.printStackTrace();
		}

		IndexTable indextable = new IndexTable("real_table_with_index");
		indextable.setScannerCaching(10000);
		indextable.setMaxScanThreads(10);
		System.out.println("max thread:" + indextable.getMaxScanThreads());

		String startvalue = "1994-01-01";
		String endvalue = "1994-01-30";

		String svalue = "100000";
		String evalue = "300000";

		Range[] ranges = new Range[2];
		ranges[0] = new Range(indextable.getTableName(), Bytes.toBytes("f:c4"));
		ranges[0].setStartType(CompareOp.GREATER_OR_EQUAL);
		ranges[0].setStartValue(Bytes.toBytes(startvalue));
		ranges[0].setEndType(CompareOp.LESS);
		ranges[0].setEndValue(Bytes.toBytes(endvalue));

		ranges[1] = new Range(indextable.getTableName(), Bytes.toBytes("f:c1"));
		ranges[1].setStartType(CompareOp.GREATER_OR_EQUAL);
		ranges[1].setStartValue(Bytes.toBytes(svalue));
		ranges[1].setEndType(CompareOp.LESS);
		ranges[1].setEndValue(Bytes.toBytes(evalue));

		byte[][] resultcolumn = new byte[1][];
		resultcolumn[0] = Bytes.toBytes("f:c4");
//		resultcolumn[1] = Bytes.toBytes("f:c1");

		try {
			IndexResultScanner rs = indextable.getScanner(
					new Range[][] { ranges }, resultcolumn);

			Result r;

			Map<byte[], DataType> columnMap = indextable.getColumnInfoMap();

			// System.out.println(rs.getTotalScannerNum() +
			// "   "+rs.getTotalCount() +"  "+rs.getFinishedScannerNum());
			long count = 0;
			while ((r = rs.next()) != null) {
				count++;

//				 println_test(r);
				// long tookout = rs.getTookOutCount();
				// long delaytmp = System.currentTimeMillis() - starttime1;
				// FileWriter file_writer = new FileWriter(datasource, true);
				// file_writer.write(tookout + "   " + Long.toString(delaytmp)
				// + '\n');
				// file_writer.close();
				// rs.close();
				// break;
				// }

			}
			System.out.println("total count : " + count);
		} catch (IndexNotExistedException e) {
			System.err.println("error query");
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();
		queryTest();
		long endTime = System.currentTimeMillis();
		System.out.println("endtime - starttime = " + (endTime - startTime)
				+ " ms");
	}

}
