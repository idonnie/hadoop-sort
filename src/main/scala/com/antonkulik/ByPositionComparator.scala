package com.antonkulik

import org.apache.hadoop.io.WritableComparable
import org.apache.hadoop.io.WritableComparator

class ByPositionComparator(keyClass: Class[_ <: WritableComparable[_]], createInstances: Boolean) extends WritableComparator(keyClass, createInstances) {

	def this() = this(classOf[IntLongPair], true)		

	override
	def compare(tp1: WritableComparable[_], tp2: WritableComparable[_]): Int = {
		var cmp = (tp1.asInstanceOf[IntLongPair]).i - (tp2.asInstanceOf[IntLongPair]).i
		if (cmp == 0) {
			cmp = ((tp1.asInstanceOf[IntLongPair]).l - (tp2.asInstanceOf[IntLongPair]).l).asInstanceOf[Int]
		}
		cmp		
	}

}