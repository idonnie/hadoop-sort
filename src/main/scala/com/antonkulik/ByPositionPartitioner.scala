package com.antonkulik

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.Partitioner

class ByPositionPartitioner extends Partitioner[IntLongPair, NullWritable] {
	
	def getPartition(pair: IntLongPair, nullWritable: NullWritable, numPartitions: Int): Int = {
		val l = pair.l
		var i = (l ^ (l >>> 32)).asInstanceOf[Int]
		if (i < 0) {
			i = -i
			if (i < 0) {
			    i = Int.MaxValue
			}
		}
		i % numPartitions		
	}

	override
	def configure(arg0: JobConf) = arg0.setPartitionerClass(this.getClass())		

}