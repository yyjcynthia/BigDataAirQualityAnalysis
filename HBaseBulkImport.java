import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;


public class HBaseBulkImport {

	public static class HbaseImportMapper extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] items = line.split(",", -1);

			ImmutableBytesWritable rowkeyWritable;
			
			Random random = new Random();
			int num = random.nextInt(10000)+1;
			String snum = Integer.toString(num);
			
			while (snum.length ()< 4){
				snum  = "0" + snum;
			}

			String sRowKey = snum +
					items[0] + "/" + items[1] + "/" + items[2] + "-"
					+ items[3] + ":" + items[4] + "-" + items[5] + "-"
					+ items[6] + "-" + items[8] + "-" + items[9];

			byte[] rowKey = Bytes.toBytes(sRowKey);

			rowkeyWritable = new ImmutableBytesWritable(rowKey);

			// year
			KeyValue kv_year = new KeyValue(rowKey, Bytes.toBytes("date"),
					Bytes.toBytes("year"), System.currentTimeMillis(),
					Bytes.toBytes(items[0]));

			if (null != kv_year) {
				context.write(rowkeyWritable, kv_year);
			}

			// date:month
			KeyValue kv_month = new KeyValue(rowKey, Bytes.toBytes("date"),
					Bytes.toBytes("month"), System.currentTimeMillis(),
					Bytes.toBytes(items[1]));

			if (null != kv_month) {
				context.write(rowkeyWritable, kv_month);
			}

			// date:day
			KeyValue kv_day = new KeyValue(rowKey, Bytes.toBytes("date"),
					Bytes.toBytes("day"), System.currentTimeMillis(),
					Bytes.toBytes(items[2]));

			if (null != kv_day) {
				context.write(rowkeyWritable, kv_day);
			}
			
			// date:hour
			KeyValue kv_hour = new KeyValue(rowKey, Bytes.toBytes("date"),
					Bytes.toBytes("hour"), System.currentTimeMillis(),
					Bytes.toBytes(items[3]));

			if (null != kv_hour) {
				context.write(rowkeyWritable, kv_hour);
			}
			
			// date:min
			KeyValue kv_minute = new KeyValue(rowKey, Bytes.toBytes("date"),
					Bytes.toBytes("minute"), System.currentTimeMillis(),
					Bytes.toBytes(items[4]));

			if (null != kv_minute) {
				context.write(rowkeyWritable, kv_minute);
			}
			
			// location:region
			KeyValue kv_region = new KeyValue(rowKey, Bytes.toBytes("location"),
					Bytes.toBytes("region"), System.currentTimeMillis(),
					Bytes.toBytes(items[5]));

			if (null != kv_region) {
				context.write(rowkeyWritable, kv_region);
			}
			
			// location:region
			KeyValue kv_site = new KeyValue(rowKey, Bytes.toBytes("location"),
					Bytes.toBytes("site"), System.currentTimeMillis(),
					Bytes.toBytes(items[8]));

			if (null != kv_site) {
				context.write(rowkeyWritable, kv_site);
			}
			
			// data:cams
			KeyValue kv_cams = new KeyValue(rowKey, Bytes.toBytes("data"),
					Bytes.toBytes("cams"), System.currentTimeMillis(),
					Bytes.toBytes(items[9]));

			if (null != kv_cams) {
				context.write(rowkeyWritable, kv_cams);
			}
										
			// data:param_id
			KeyValue kv_param_id = new KeyValue(rowKey, Bytes.toBytes("data"),
					Bytes.toBytes("param_id"), System.currentTimeMillis(),
					Bytes.toBytes(items[6]));

			if (null != kv_param_id) {
				context.write(rowkeyWritable, kv_param_id);
			}
			
			// data:param_name
			KeyValue kv_param_name = new KeyValue(rowKey, Bytes.toBytes("data"),
					Bytes.toBytes("param_name"), System.currentTimeMillis(),
					Bytes.toBytes(items[7]));

			if (null != kv_param_name) {
				context.write(rowkeyWritable, kv_param_name);
			}		
			
			// data:value
			KeyValue kv_value = new KeyValue(rowKey, Bytes.toBytes("data"),
					Bytes.toBytes("value"), System.currentTimeMillis(),
					Bytes.toBytes(items[10]));

			if (null != kv_value) {
				context.write(rowkeyWritable, kv_value);
			}		
			
			// data:flag
			KeyValue kv_flag = new KeyValue(rowKey, Bytes.toBytes("data"),
					Bytes.toBytes("flag"), System.currentTimeMillis(),
					Bytes.toBytes(items[11]));

			if (null != kv_flag) {
				context.write(rowkeyWritable, kv_flag);
			}	
			
			/*
			 * Put put=new Put(rowKey);
			 * 
			 * put.add(Bytes.toBytes("date"),Bytes.toBytes("year"),
			 * Bytes.toBytes(items[0]));
			 * put.add(Bytes.toBytes("date"),Bytes.toBytes("month"),
			 * Bytes.toBytes(items[1]));
			 * put.add(Bytes.toBytes("date"),Bytes.toBytes("day"),
			 * Bytes.toBytes(items[2]));
			 * put.add(Bytes.toBytes("date"),Bytes.toBytes("hour"),
			 * Bytes.toBytes(items[3]));
			 * put.add(Bytes.toBytes("date"),Bytes.toBytes("minute"),
			 * Bytes.toBytes(items[4]));
			 * put.add(Bytes.toBytes("location"),Bytes.toBytes("region"),
			 * Bytes.toBytes(items[5]));
			 * put.add(Bytes.toBytes("location"),Bytes.toBytes("site"),
			 * Bytes.toBytes(items[6]));
			 * put.add(Bytes.toBytes("data"),Bytes.toBytes("cams"),
			 * Bytes.toBytes(items[7]));
			 * put.add(Bytes.toBytes("data"),Bytes.toBytes("param_id"),
			 * Bytes.toBytes(items[8]));
			 * put.add(Bytes.toBytes("data"),Bytes.toBytes("param_name"),
			 * Bytes.toBytes(items[9]));
			 * put.add(Bytes.toBytes("data"),Bytes.toBytes("value"),
			 * Bytes.toBytes(items[10]));
			 * put.add(Bytes.toBytes("data"),Bytes.toBytes("flag"),
			 * Bytes.toBytes(items[11])); context.write(rowkeyWritable, put);
			 */
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration confj = new Configuration();

		String[] otherArgs = new GenericOptionsParser(confj, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		Job job = new Job(confj, "hBasePractice");

		job.setJarByClass(HBaseBulkImport.class);
		job.setMapperClass(HbaseImportMapper.class);
		job.setReducerClass(KeyValueSortReducer.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(KeyValue.class);
		job.setOutputFormatClass(HFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		job.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		int reducer_number = Integer.parseInt(otherArgs[2].toString());
		job.setNumReduceTasks(reducer_number);
	
		Configuration conf = HBaseConfiguration.create();

		int finish = (job.waitForCompletion(true) ? 0 : 1);
		
		System.out.println("HfileCreatedStatus" + finish);

		HTable table = new HTable(conf, "bigd02-hbase-sample");
		HFileOutputFormat.configureIncrementalLoad(job, table);
		LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
		loader.doBulkLoad(new Path(otherArgs[1]), table);
	}
}