package com.antonkulik

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobClient
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.fs.Path


object Sort {
	

	def main(args: Array[String]): Unit = {
		
		// config
		val conf = new JobConf(classOf[SortJava])
		conf.setJobName("wordcount")
		
		// map 
		conf.setMapOutputKeyClass(classOf[IntLongPair])
		conf.setMapOutputValueClass(classOf[NullWritable])
		
		// reduce
		conf.setOutputKeyClass(classOf[Text])
		conf.setOutputValueClass(classOf[NullWritable])

		// Partitioner and Comparator for secondary index
		conf.setPartitionerClass(classOf[ByPositionPartitioner])
        conf.setOutputKeyComparatorClass(classOf[ByPositionComparator])
		
		conf.setMapperClass(classOf[Map])
		conf.setReducerClass(classOf[Reduce])
        
		conf.setInputFormat(classOf[TextInputFormat])
		conf.setOutputFormat(classOf[TextOutputFormat[_,_]])

		FileInputFormat.setInputPaths(conf, new Path(args(0)))
		FileOutputFormat.setOutputPath(conf, new Path(args(1)))

		JobClient.runJob(conf)
	}
	 
	
}