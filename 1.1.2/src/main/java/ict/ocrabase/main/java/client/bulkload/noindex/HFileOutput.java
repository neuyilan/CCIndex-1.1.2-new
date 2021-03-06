package ict.ocrabase.main.java.client.bulkload.noindex;
import ict.ocrabase.main.java.client.bulkload.KeyValueArray;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileWriterV3;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * Used for writing KeyValue into HFile
 * @author gu
 *
 */
public class HFileOutput extends
		FileOutputFormat<Text, KeyValueArray> {		
	static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";

	public RecordWriter<Text, KeyValueArray> getRecordWriter(
			final TaskAttemptContext context) throws IOException,
			InterruptedException {
		int attemptID = context.getTaskAttemptID().getId();
		int taskID = context.getTaskAttemptID().getTaskID().getId();
		final String suffix = String.format("%d_%d_", taskID, attemptID);
		// Get the path of the temporary output file
		final Path outputPath = FileOutputFormat.getOutputPath(context);
		final Path outputdir = new FileOutputCommitter(outputPath, context)
				.getWorkPath();
		Configuration conf = context.getConfiguration();
		final FileSystem fs = outputdir.getFileSystem(conf);

		// These configs. are from hbase-*.xml
		final long maxsize = conf.getLong("hbase.hregion.max.filesize",268435456);
		final int blocksize = conf.getInt("hbase.mapreduce.hfileoutputformat.blocksize", 65536);
		
		// Invented config.  Add to hbase-*.xml if other than default compression.
	    final String defaultCompression = conf.get("hfile.compression",
	        Compression.Algorithm.NONE.getName());

	    // create a map from column family to the compression algorithm
	    final Map<byte[], String> compressionMap = createFamilyCompressionMap(conf);
		

		return new RecordWriter<Text, KeyValueArray>() {
			// Map of families to writers and how much has been output on the
			// writer.
			private final Map<byte[], WriterLength> writers = new TreeMap<byte[], WriterLength>(
					Bytes.BYTES_COMPARATOR);
			private byte[] previousRow = HConstants.EMPTY_BYTE_ARRAY;
			private boolean spilitRequested = false;
			private String tName = null;
			
			private int round = 0;

			public void write(Text tableName, KeyValueArray kvList)
					throws IOException {
				if(tName == null)
					tName = tableName.toString();
				if (spilitRequested) {
					rollWriter();
					round++;
					spilitRequested = false;
				}

				KeyValue[] kvs = kvList.get();
				for (KeyValue kv : kvs) {
					
					byte[] rowKey = kv.getRow();
					long length = kv.getLength();
					byte[] family = kv.getFamily();
					WriterLength wl = this.writers.get(family);
					if (wl == null) {
						Path basedir = new Path(outputdir, tableName.toString() + "/" + Bytes
								.toString(family));
						if (!fs.exists(basedir))
							fs.mkdirs(basedir);
					}
					

					if (wl == null || wl.writer == null) {
						wl = new WriterLength();
						Path basedir = new Path(outputdir, tableName.toString() + "/" + Bytes
								.toString(family));
						wl.writer = getNewWriter(family, basedir);
						this.writers.put(family, wl);

					}
					if ((length + wl.written) >= maxsize
							&& Bytes.compareTo(this.previousRow, rowKey) != 0) {
						spilitRequested = true;
					}
					
					//kv.updateLatestStamp(this.now);
					wl.writer.append(kv);
					wl.written += length;
					// Copy the row so we know when a row transition.
					this.previousRow = kv.getRow();

				}

			}

			/**
			 * Close all writers and create new writers
			 * @throws IOException
			 */
			private void rollWriter() throws IOException {
				for (Map.Entry<byte[], WriterLength> wlEntry : writers
						.entrySet()) {
					WriterLength wl = wlEntry.getValue();
					if (wl.writer == null)
						continue;
					System.out.println(wl.writer.getPath().toString());
					close(wl.writer);
					
					wl.writer = null;
				}
			}
			

			/**
			 * Create a new HFile.Writer. Close current if there is one.
			 * @param family 
			 * @param writer
			 * @param familydir
			 * @return A new HFile.Writer.
			 * @throws IOException
			 */
			private HFile.Writer getNewWriter(
					byte[] family, final Path familydir) throws IOException {
				String compression = compressionMap.get(family);
		        compression = compression == null ? defaultCompression : compression;

        final Configuration conf = context.getConfiguration();
        HFileContext hfile_context = new HFileContext();

        return new HFileWriterV3(conf, new CacheConfig(conf), fs, new Path(familydir, suffix + round), null,
            KeyValue.COMPARATOR, hfile_context);
        // HFile.Writer(fs, new Path(familydir, suffix+round), blocksize, compression,
        // KeyValue.COMPARATOR);
			}

			/**
			 * Close the HFile Writer
			 * @param w the Writer you want to close
			 * @throws IOException
			 */
			private void close(final HFile.Writer w) throws IOException {
				if (w != null) {
					w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY, Bytes
							.toBytes(System.currentTimeMillis()));
					w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY, Bytes
							.toBytes(context.getTaskAttemptID().toString()));
					w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY, Bytes
							.toBytes(true));
					w.close();
				}
			}

			/**
			 * Close this HFile
			 */
			public void close(TaskAttemptContext c) throws IOException {
				// close(e.getValue().writer);
				if (!writers.isEmpty())
					rollWriter();
				//writer.close();
			}
		};
	}

	/**
	 * This class store the HFile Wirter and how many bytes that is written
	 * @author gu
	 *
	 */
	static class WriterLength {
		long written = 0;
		HFile.Writer writer = null;
	}

	/**
	 * Used for partition, get all the region's startkeys of the specific table
	 * @param table specific the table
	 * @return the startkeys of all the regions
	 * @throws IOException
	 */
	public static List<ImmutableBytesWritable> getRegionStartKeys(HTable table)
			throws IOException {
		byte[][] byteKeys = table.getStartKeys();
		ArrayList<ImmutableBytesWritable> ret = new ArrayList<ImmutableBytesWritable>(
				byteKeys.length);
		for (byte[] byteKey : byteKeys) {
			ret.add(new ImmutableBytesWritable(byteKey));
		}
		return ret;
	}

	/**
	 * According to the startkeys, write the partition into the partition file
	 * @param conf 
	 * @param startKeys the startkeys of all the regions
	 * @throws IOException
	 */
	public static void writePartitions(Configuration conf,
			List<ImmutableBytesWritable> startKeys) throws IOException {
		if (startKeys.isEmpty()) {
			throw new IllegalArgumentException("No regions passed");
		}

		// We're generating a list of split points, and we don't ever
		// have keys < the first region (which has an empty start key)
		// so we need to remove it. Otherwise we would end up with an
		// empty reducer with index 0
		TreeSet<ImmutableBytesWritable> sorted = new TreeSet<ImmutableBytesWritable>(
				startKeys);

		ImmutableBytesWritable first = sorted.first();
//		if (!first.equals(HConstants.EMPTY_BYTE_ARRAY)) {
//			throw new IllegalArgumentException(
//					"First region of table should have empty start key. Instead has: "
//							+ Bytes.toStringBinary(first.get()));
//		}
		sorted.remove(first);

		Path dst = new Path(TotalOrderPartitioner.getPartitionFile(conf));
		FileSystem fs = dst.getFileSystem(conf);
		if (fs.exists(dst)) {
			fs.delete(dst, false);
		}
		// Write the actual file
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, dst,
				ImmutableBytesWritable.class, NullWritable.class);

		try {
			for (ImmutableBytesWritable startKey : sorted) {
				writer.append(startKey, NullWritable.get());
			}
		} finally {
			writer.close();
		}
	}
	
	/**
	   * Run inside the task to deserialize column family to compression algorithm
	   * map from the
	   * configuration.
	   * 
	   * Package-private for unit tests only.
	   * 
	   * @return a map from column family to the name of the configured compression
	   *         algorithm
	   */
	  static Map<byte[], String> createFamilyCompressionMap(Configuration conf) {
	    Map<byte[], String> compressionMap = new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR);
	    String compressionConf = conf.get(COMPRESSION_CONF_KEY, "");
	    for (String familyConf : compressionConf.split("&")) {
	      String[] familySplit = familyConf.split("=");
	      if (familySplit.length != 2) {
	        continue;
	      }
	      
	      try {
	        compressionMap.put(URLDecoder.decode(familySplit[0], "UTF-8").getBytes(),
	            URLDecoder.decode(familySplit[1], "UTF-8"));
	      } catch (UnsupportedEncodingException e) {
	        // will not happen with UTF-8 encoding
	        throw new AssertionError(e);
	      }
	    }
	    return compressionMap;
	  }

	  /**
	   * Serialize column family to compression algorithm map to configuration.
	   * Invoked while configuring the MR job for incremental load.
	   * 
	   * Package-private for unit tests only.
	   * 
	   * @throws IOException
	   *           on failure to read column family descriptors
	   */
	 public static void configureCompression(HTable table, Configuration conf) throws IOException {
	    StringBuilder compressionConfigValue = new StringBuilder();
	    HTableDescriptor tableDescriptor = table.getTableDescriptor();
	    if(tableDescriptor == null){
	      // could happen with mock table instance
	      return;
	    }
	    Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
	    int i = 0;
	    for (HColumnDescriptor familyDescriptor : families) {
	      if (i++ > 0) {
	        compressionConfigValue.append('&');
	      }
	      compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
	      compressionConfigValue.append('=');
	      compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getCompression().getName(), "UTF-8"));
	    }
	    // Get rid of the last ampersand
	    conf.set(COMPRESSION_CONF_KEY, compressionConfigValue.toString());
	  }
}
