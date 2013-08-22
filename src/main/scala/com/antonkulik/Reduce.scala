package com.antonkulik

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.MapReduceBase
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.Reducer
import org.apache.hadoop.mapred.Reporter
import java.util.{Iterator => JavaIterator}

class Reduce extends MapReduceBase with Reducer[IntLongPair, NullWritable, Text, NullWritable] {
    
    override
	def reduce(key: IntLongPair, values: JavaIterator[NullWritable],  output: OutputCollector[Text, NullWritable], reporter: Reporter ): Unit = {				
		output.collect(new Text(String.valueOf(key.i)), NullWritable.get)
	}
    
}
