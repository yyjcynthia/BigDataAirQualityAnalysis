import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

public class AirQualityO3s {

	public static class TwoKeyWritable implements
			WritableComparable<TwoKeyWritable> {

		private Text sitekey;
		private Text timekey;

		public TwoKeyWritable() {
			this.sitekey = new Text();
			this.timekey = new Text();
		}

		// @Override
		public void set(String a, String b) {
			this.sitekey.set(a);
			this.timekey.set(b);
		}

		public Text getsitekey() {
			return sitekey;
		}

		public Text gettimekey() {
			return timekey;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			sitekey.write(out);
			timekey.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			sitekey.readFields(in);
			timekey.readFields(in);
		}

		public int compareTo(TwoKeyWritable tp) {
			int cmp = sitekey.compareTo(tp.sitekey);
			if (cmp != 0) {
				return cmp;
			}
			return timekey.compareTo(tp.timekey);
		}

		@Override
		public String toString() {
			return sitekey + "\t" + timekey;
		}

		@Override
		public int hashCode() {
			return sitekey.hashCode() % 10 + timekey.hashCode();
		}
	}

	public static class FilterMapper extends
			TableMapper<TwoKeyWritable, DoubleWritable> {

		private TwoKeyWritable twoKey = new TwoKeyWritable();
		private DoubleWritable outvalue = new DoubleWritable();

		String[] siteID = { "1", "8", "18", "22", "26", "35", "48", "51", "53",
				"81", "108", "110", "139", "146", "150", "154", "169", "181",
				"235", "240", "404", "405", "406", "407", "409", "410", "411",
				"671", "673", "695", "697", "698", "699", "1001" };

		public void map(ImmutableBytesWritable row, Result values,
				Context context) throws IOException, InterruptedException {

			boolean mapflag = true; // for checking if the flag is empty
			boolean siteflag = false;  // for checking if it is houston site
			String[] datekey = new String[5];
			String site = null;
			String value = null;

			for (KeyValue kv : values.list()) {

				if (Bytes.toString(kv.getFamily()).equals("data")
						&& Bytes.toString(kv.getQualifier()).equals("flag")) {
					if (!Bytes.toString(kv.getValue()).equals("\"\"")) {
						mapflag = false;

					}
				}

				if (Bytes.toString(kv.getFamily()).equals("location")
						&& Bytes.toString(kv.getQualifier()).equals("site")) {

					for (int i = 0; i < siteID.length; i++) {
						if (Bytes.toString(kv.getValue()).equals(siteID[i]))
							siteflag = true;
					}
				}

				if (Bytes.toString(kv.getFamily()).equals("date")
						&& Bytes.toString(kv.getQualifier()).equals("year")) {
					datekey[0] = Bytes.toString(kv.getValue());

				}

				if (Bytes.toString(kv.getFamily()).equals("date")
						&& Bytes.toString(kv.getQualifier()).equals("month")) {
					datekey[1] = Bytes.toString(kv.getValue());

				}

				if (Bytes.toString(kv.getFamily()).equals("date")
						&& Bytes.toString(kv.getQualifier()).equals("day")) {
					datekey[2] = Bytes.toString(kv.getValue());
				}

				if (Bytes.toString(kv.getFamily()).equals("date")
						&& Bytes.toString(kv.getQualifier()).equals("hour")) {
					datekey[3] = Bytes.toString(kv.getValue());
				}

				if (Bytes.toString(kv.getFamily()).equals("date")
						&& Bytes.toString(kv.getQualifier()).equals("minute")) {
					datekey[4] = Bytes.toString(kv.getValue());
				}

				if (Bytes.toString(kv.getFamily()).equals("location")
						&& Bytes.toString(kv.getQualifier()).equals("site")) {
					site = Bytes.toString(kv.getValue());
				}

				if (Bytes.toString(kv.getFamily()).equals("data")
						&& Bytes.toString(kv.getQualifier()).equals("value")) {

					value = Bytes.toString(kv.getValue());

				}
			}
			
			
			if (siteflag && mapflag) {

				String timekey = datekey[0] + "-" + datekey[1] + "-"
						+ datekey[2] + "-" + datekey[3];
				double dvalue = Double.parseDouble(value);
				twoKey.set(site, timekey);
				outvalue.set(dvalue);
				
				// the key has two parameters: site id, time(hour).
				context.write(twoKey, outvalue);

			} else
				return;

		}
	}

	public static class OneHourReducer
			extends
			Reducer<TwoKeyWritable, DoubleWritable, TwoKeyWritable, DoubleWritable> {

		private DoubleWritable result = new DoubleWritable();

		public void reduce(TwoKeyWritable key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			int count = 0;
			double sum = 0.0;
			
			// hour average calculation for the same keys: site id and hour
			for (DoubleWritable val : values) {

				sum = sum + val.get();
				count++;
			}

			result.set(sum / count);

			context.write(key, result);
		}

	}

	public static class EightHourFilterMapper extends
			Mapper<Object, Text, TwoKeyWritable, DoubleWritable> {

		private TwoKeyWritable twoKey = new TwoKeyWritable();
		private String line;
		private String[] temp;
		private String[] temptime;

		private DoubleWritable hourvalue = new DoubleWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
			while (itr.hasMoreTokens()) {

				line = itr.nextToken();
				temp = line.split("\t");
				temptime = temp[1].split("\\-");

				String year = temptime[0];
				String month = temptime[1];

				int daykey = 0;
				int a = Integer.parseInt(month);
				
				// translate the time to be the hour# in the year.
				// 2011-2-3-9 -> (31+3)*24+9 = 825
				switch (a) {

				case 1:
					daykey = 0;
					break;
				case 2:
					daykey = 31;
					break;
				case 3:
					daykey = 59;
					break;
				case 4:
					daykey = 90;
					break;
				case 5:
					daykey = 120;
					break;
				case 6:
					daykey = 151;
					break;
				case 7:
					daykey = 181;
					break;
				case 8:
					daykey = 212;
					break;
				case 9:
					daykey = 243;
					break;
				case 10:
					daykey = 273;
					break;
				case 11:
					daykey = 304;
					break;
				case 12:
					daykey = 334;
					break;
				default:
					daykey = 0;

				}

				int hour = ((Integer.parseInt(year)-2011)*365 +daykey + Integer.parseInt(temptime[2])) * 24
						+ Integer.parseInt(temptime[3]);

				int time = hour;
				hourvalue.set(Double.parseDouble(temp[2]));
				
				
				// Every value, will be assigned to 9 keys.
				// Example: assume the value for "site 8, hour# 150" is 10.0. 
				// Then 10.0 Will be assigned to "site 8, hour# 150", "site 8, hour 149#", ""site 8, hour 148#"
			    // "site 8, hour 147#"..... "site 8, hour 142#". So for the 8 hour rolling average calculation 
				// in reducer.
				for (int i = 0; i <= 8; i++) {
					time = time - 1;
					twoKey.set(temp[0], Integer.toString(time));
					
					context.write(twoKey, hourvalue);
				}
			}
		}
	}

	public static class EightHourReducer extends
			Reducer<TwoKeyWritable, DoubleWritable, TwoKeyWritable, Text> {

		private Text result = new Text();
		private TwoKeyWritable resultkey = new TwoKeyWritable();

		public void reduce(TwoKeyWritable key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			double sum = 0;
			int count = 0;
			String site = key.getsitekey().toString();

			// translate the hour# in the year 2011, back to be in the time 
			// format : month/day/hour - hour:00:00
			//Example:  825 -> 2011/2/3 - 9:00:00
			String time = key.timekey.toString();

			int _hour = (Integer.parseInt(time)) % 24;
			int _day = (int) Math.floor((Integer.parseInt(time)) / 24);
			String year;
			
			if(_day <0)
			{year = "2010";
			   _day = _day + 365;}
			else if (_day >365){
				year = "2012";
				_day = _day -365;
			}
			else
			{year = "2011";}
			
			
			int _month = 0;

			if (0 < _day && _day <= 31)
				_month = 1;

			if (31 < _day && _day <= 59)
				_month = 2;

			if (59 < _day && _day <= 90)
				_month = 3;

			if (90 < _day && _day <= 120)
				_month = 4;

			if (120 < _day && _day <= 151)
				_month = 5;

			if (151 < _day && _day <= 181)
				_month = 6;

			if (181 < _day && _day <= 212)
				_month = 7;

			if (212 < _day && _day <= 243)
				_month = 8;

			if (243 < _day && _day <= 273)
				_month = 9;

			if (273 < _day && _day <= 304)
				_month = 10;

			if (304 < _day && _day <= 334)
				_month = 11;

			if (334 < _day && _day <= 365)
				_month = 12;

			
			switch (_month) {

			case 1:
			case 2:
				_day = _day - 31;
				break;
			case 3:
				_day = _day - 59;
				break;
			case 4:
				_day = _day - 90;
				break;
			case 5:
				_day = _day - 120;
				break;
			case 6:
				_day = _day - 151;
				break;
			case 7:
				_day = _day - 181;
				break;
			case 8:
				_day = _day - 212;
				break;
			case 9:
				_day = _day - 243;
				break;
			case 10:
				_day = _day - 273;
				break;
			case 11:
				_day = _day - 304;
				break;
			case 12:
				_day = _day - 334;
				break;
			}

			String _time = (year + "/" + _month + "/" + _day + "-" + _hour + ":00:00");

			resultkey.set(site, _time);

			for (DoubleWritable val : values)

			{
				sum = sum + val.get();
				count++;
			}

			if (count == 9) {
				double dresult = sum / 9;

				result.set(String.valueOf(dresult));
			} else {
				result.set("NA");
			}

			context.write(resultkey, result);
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		Configuration conf = HBaseConfiguration.create();

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.exit(2);
		}

		Job job = new Job(conf, "HoursAverage");

		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setJarByClass(AirQualityO3s.class);
		job.setMapperClass(FilterMapper.class);
		job.setReducerClass(OneHourReducer.class);
		job.setMapOutputKeyClass(TwoKeyWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		Scan scan = new Scan();

		/*
		 * FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		 * SingleColumnValueFilter[] filters; filters = new
		 * SingleColumnValueFilter[36]; for (int i = 0; i < siteID.length; i++)
		 * {
		 * 
		 * filters[i] = new SingleColumnValueFilter("location".getBytes(),
		 * "site".getBytes(), CompareOp.EQUAL, Bytes.toBytes(siteID[i]));
		 * 
		 * list.addFilter(filters[i]);
		 * 
		 * System.out.println("filter" + i + ":" + siteID[i]); }
		 */
		/*
		 * FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		 * filters = new SingleColumnValueFilter[2]; filters[0] = new
		 * SingleColumnValueFilter("data".getBytes(), "param_name".getBytes(),
		 * CompareOp.EQUAL, Bytes.toBytes("\"co\""));
		 * 
		 * filters[1] = new SingleColumnValueFilter("data".getBytes(),
		 * "flag".getBytes(), CompareOp.EQUAL, Bytes.toBytes("\"\""));
		 * 
		 * list.addFilter(filters[0]); list.addFilter(filters[1]);
		 */
		

		// one filter is runable in our current hbase.
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				"data".getBytes(), "param_name".getBytes(), CompareOp.EQUAL,
				Bytes.toBytes("\"o3\""));

		scan.setFilter(filter);
		scan.setCaching(500);
		scan.setCacheBlocks(false);

		TableMapReduceUtil.initTableMapperJob(otherArgs[0], scan,
				FilterMapper.class, TwoKeyWritable.class, DoubleWritable.class,
				job);

		job.waitForCompletion(true);

		// Start the job2
		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "EightHoursEverage");

		job2.setJarByClass(AirQualityO3s.class);

		job2.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));

		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

		job2.setMapperClass(EightHourFilterMapper.class);
		job2.setReducerClass(EightHourReducer.class);
		job2.setMapOutputKeyClass(TwoKeyWritable.class);
		job2.setMapOutputValueClass(DoubleWritable.class);

		job2.waitForCompletion(true);

	}
}
