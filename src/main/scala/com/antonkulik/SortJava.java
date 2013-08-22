package com.antonkulik;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class SortJava {
	
	private static final NullWritable nil = NullWritable.get();
	
	public static class IntLongPair implements WritableComparable<IntLongPair> {
		
		private int i;
		
		private long l;
		
		public IntLongPair() {
			super();
		}
		
		public IntLongPair(final int i, final long l) {
			super();
			this.i = i;
			this.l = l;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {			
			i = arg0.readInt();
			l = arg0.readLong();
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			arg0.writeInt(i);
			arg0.writeLong(l);			
		}

		@Override
		public int compareTo(IntLongPair o) {
			 long compareValue = i - o.i;
			 if (compareValue == 0) {
				 compareValue = l - o.l;
			 }
			 return Long.signum(compareValue);
		}
		
		protected boolean canEqual(Object o) {
			return o instanceof IntLongPair;
		}
		
		@Override
		public boolean equals(Object o) {
			boolean canEqual = o != null && canEqual(o);
			if (!canEqual) {
				return false;
			}
			IntLongPair pair = (IntLongPair) o;
			return i == pair.i && l == pair.l;
		}
		
		@Override
		public int hashCode() {
			return i ^ (int)(l ^ (l >>> 32));
		}
		
	}
	
	public static class ByPositionPartitioner implements Partitioner<IntLongPair, NullWritable> {
		
		public ByPositionPartitioner() {
			super();
		}
	
		public int getPartition(IntLongPair pair, NullWritable nullWritable, int numPartitions) {
			long l = pair.l;
			int i = (int)(l ^ (l >>> 32));
			if (i < 0) {
				i = -i;
				if (i < 0) {
					i = Integer.MAX_VALUE;
				}
			}
		    return i % numPartitions;		
		}

		@Override
		public void configure(JobConf arg0) {
			arg0.setPartitionerClass(this.getClass());
		}

	}
	
	public static class ByPositionComparator extends WritableComparator {
		
	    public ByPositionComparator() {
		    super(IntLongPair.class, true);		
		}
		
		@Override
		public int compare(WritableComparable tp1, WritableComparable tp2) {
			int cmp = ((IntLongPair)tp1).i - ((IntLongPair)tp2).i;
			if (cmp == 0) {
				cmp = (int) (((IntLongPair)tp1).l - ((IntLongPair)tp2).l);
			}
		    return cmp;		
	    }
		
    }
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntLongPair, NullWritable> {

		public void map(LongWritable key, Text value, OutputCollector<IntLongPair, NullWritable> output, Reporter reporter) throws IOException {
			String valueStr = value.toString().trim();
			if (! (valueStr.length() > 0)) {
				return;
			}
			output.collect(new IntLongPair(Integer.parseInt(valueStr), key.get()), nil);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntLongPair, NullWritable, Text, NullWritable> {
		public void reduce(IntLongPair key, Iterator<NullWritable> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {			
			output.collect(new Text(String.valueOf(key.i)), nil);
		}
	}

	public static void main(String[] args) throws Exception {
		
		// config
		JobConf conf = new JobConf(SortJava.class);
		conf.setJobName("wordcount");
		
		// map 
		conf.setMapOutputKeyClass(IntLongPair.class);
		conf.setMapOutputValueClass(NullWritable.class);
		
		// reduce
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);

		// Partitioner and Comparator for secondary index
		conf.setPartitionerClass(ByPositionPartitioner.class);
        conf.setOutputKeyComparatorClass(ByPositionComparator.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
        
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}