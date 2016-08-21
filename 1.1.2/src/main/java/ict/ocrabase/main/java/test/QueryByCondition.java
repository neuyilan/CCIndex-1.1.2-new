package ict.ocrabase.main.java.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class QueryByCondition {
	private static Configuration conf;
	private static final byte[] table_test = Bytes
			.toBytes("real_table_with_index");
	private static long startTime = 0;
	private static long stopTime = 0;
	static int count = 0;

	public void queryByRowKey(String rowKey) throws IOException {
		conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, table_test);
		Get get = new Get(rowKey.getBytes());
		Result r = table.get(get);
		System.out.println("rowkey: " + new String(r.getRow()));
		for (KeyValue kv : r.raw()) {
			System.out.println("column: "
					+ new String(kv.getFamily() + " ====value:  "
							+ new String(kv.getValue())));
		}
	}

	public void queryByColumnValue(String columnValue) throws IOException {
		conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, table_test);
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes("f"),
				Bytes.toBytes("c4"), CompareOp.EQUAL,
				Bytes.toBytes(columnValue));
		Scan s = new Scan();
		s.setFilter(filter);
		ResultScanner rs = table.getScanner(s);

		for (Result r : rs) {
			System.out.println("rowkey: " + new String(r.getRow()));
			for (KeyValue kv : r.raw()) {
				System.out.println("column: " + new String(kv.getFamily())
						+ "====value: " + new String(kv.getValue()));
			}
		}
	}

	public void queryByColumnRange(String startValue, String endValue,String svalue,String evalue)
			throws IOException {
		conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, table_test);
		List<Filter> filters = new ArrayList<Filter>();

		Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("f"),
				Bytes.toBytes("c4"), CompareOp.GREATER_OR_EQUAL,
				Bytes.toBytes(startValue));
		filters.add(filter1);
		
		Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("f"),
				Bytes.toBytes("c4"), CompareOp.LESS, Bytes.toBytes(endValue));
		filters.add(filter2);
		
		Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("f"),
				Bytes.toBytes("c1"), CompareOp.GREATER_OR_EQUAL,
				Bytes.toBytes(svalue));
		filters.add(filter3);
		
		Filter filter4 = new SingleColumnValueFilter(Bytes.toBytes("f"),
				Bytes.toBytes("c1"), CompareOp.LESS, Bytes.toBytes(evalue));
		filters.add(filter4);
		
		Scan s = new Scan();
		FilterList filterList1 = new FilterList(filters);
		
		s.setFilter(filterList1);
		
		ResultScanner rs = table.getScanner(s);
		long count = 0;
		for (Result r : rs) {
//			println_test(r);
			count++;
		}
		System.out.println("total count = " + count);
	}

	public void println_test(Result result) {
		StringBuilder sb = new StringBuilder();
		sb.append("row=" + Bytes.toString(result.getRow()));

//		List<KeyValue> kv = result.getColumn(Bytes.toBytes("f"),
//				Bytes.toBytes("c1"));
//		if (kv.size() != 0) {
//			sb.append(", f:c1=" + Bytes.toInt(kv.get(0).getValue()));
//		}
//
//		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c2"));
//		if (kv.size() != 0) {
//			sb.append(", f:c2=" + Bytes.toString(kv.get(0).getValue()));
//		}
//
//		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c3"));
//		if (kv.size() != 0) {
//			sb.append(", f:c3=" + Bytes.toDouble(kv.get(0).getValue()));
//		}
//
//		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c4"));
//		if (kv.size() != 0) {
//			sb.append(", f:c4=" + Bytes.toString(kv.get(0).getValue()));
//		}
//		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c5"));
//		if (kv.size() != 0) {
//			sb.append(", f:c5=" + Bytes.toString(kv.get(0).getValue()));
//		}
//
//		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c6"));
//		if (kv.size() != 0) {
//			sb.append(", f:c6=" + Bytes.toString(kv.get(0).getValue()));
//		}
//		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c7"));
//		if (kv.size() != 0) {
//			sb.append(", f:c7=" + Bytes.toInt(kv.get(0).getValue()));
//		}
//		kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c8"));
//		if (kv.size() != 0) {
//			sb.append(", f:c8=" + Bytes.toString(kv.get(0).getValue()));
//		}
		
		
		List<KeyValue>  kv = result.getColumn(Bytes.toBytes("f"), Bytes.toBytes("c4"));
		if (kv.size() != 0) {
			sb.append(", f:c4=" + Bytes.toString(kv.get(0).getValue()));
		}
		
		System.out.println(sb.toString());
	}

	public static void main(String args[]) {
		QueryByCondition queryByCondition = new QueryByCondition();
		
		String svalue = "100000";
		String evalue = "300000";
		
		String startValue = "1994-01-01";
		String endValue = "1994-01-30";
		long startTime = System.currentTimeMillis();

		try {
			// queryByCondition.queryByColumnValue(columnValue);
			// queryByCondition.queryByRowKey(rowKey);
			queryByCondition.queryByColumnRange(startValue, endValue,svalue,evalue);
		} catch (IOException e) {
			e.printStackTrace();
		}

		long endTime = System.currentTimeMillis();
		System.out.println("endtime - starttime = " + (endTime - startTime)
				+ " ms");
	}

}
