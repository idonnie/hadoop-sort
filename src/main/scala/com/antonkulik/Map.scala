package com.antonkulik

import org.apache.hadoop.mapred.MapReduceBase
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.Mapper
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.NullWritable

class Map extends MapReduceBase with Mapper[LongWritable, Text, IntLongPair, NullWritable] {

    override
	def map(key: LongWritable, value: Text,  output: OutputCollector[IntLongPair, NullWritable],  reporter: Reporter): Unit = {
		val valueStr = value.toString.trim
		if (! (valueStr.length > 0)) {
			return
		}
		output.collect(new IntLongPair(Integer.parseInt(valueStr), key.get()), NullWritable.get)
	}
}
