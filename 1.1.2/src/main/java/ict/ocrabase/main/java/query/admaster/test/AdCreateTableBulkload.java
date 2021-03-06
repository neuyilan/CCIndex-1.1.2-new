package ict.ocrabase.main.java.query.admaster.test;

import ict.ocrabase.main.java.client.index.IndexAdmin;
import ict.ocrabase.main.java.client.index.IndexExistedException;
import ict.ocrabase.main.java.client.index.IndexSpecification;
import ict.ocrabase.main.java.client.index.IndexTableDescriptor;
import ict.ocrabase.main.java.client.index.IndexSpecification.IndexType;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class AdCreateTableBulkload {
	private static final byte[] tableName = Bytes.toBytes("ads");        
	private static Configuration conf;
  private static IndexAdmin indexadmin;
  private static HBaseAdmin admin;
  private static HTableDescriptor desc;
  private static IndexTableDescriptor indexDesc;
  private static IndexSpecification[] index;
  
  
  public static void main(String[] args) throws IOException, IndexExistedException {
    conf = HBaseConfiguration.create();
    indexadmin = new IndexAdmin(conf);
    admin = new HBaseAdmin(conf);

    desc = new HTableDescriptor(TableName.valueOf(tableName));
    
    HColumnDescriptor h1=new HColumnDescriptor(Bytes.toBytes("f"));
    h1.setMaxVersions(10);
	h1.setMinVersions(3);
	desc.addFamily(h1);
    
    index = new IndexSpecification[2];
    index[0] = new IndexSpecification(Bytes.toBytes("f:c14"), IndexType.CCINDEX);
    index[1] = new IndexSpecification(Bytes.toBytes("f:c35"), IndexType.CCINDEX);
    

     indexDesc = new IndexTableDescriptor(desc, index);
    
    
	 if (admin.tableExists(tableName)) {
	      admin.disableTable(tableName);
	      admin.deleteTable(tableName);
	    }
    
    indexadmin.createTable(indexDesc);

    System.out.println("Create table finished!");
    return;
  }
}
