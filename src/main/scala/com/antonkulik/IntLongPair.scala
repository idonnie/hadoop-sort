package com.antonkulik

import java.io.DataOutput
import org.apache.hadoop.io.WritableComparable
import java.io.DataInput

class IntLongPair extends WritableComparable[IntLongPair] {

	var i: Int = _

	var l: Long = _

	def IntLongPair() = {}

	def this(i: Int, l: Long) {
		this()
		this.i = i
		this.l = l
	}

	override
	def readFields(arg0: DataInput) = {			
		i = arg0.readInt
		l = arg0.readLong
	}

	override
	def write(arg0: DataOutput) = {
		arg0.writeInt(i)
		arg0.writeLong(l)
	}

	override
	def compareTo(o: IntLongPair): Int = {
		val compareValue = i - o.i			 
		if (compareValue != 0) compareValue else (l - o.l).asInstanceOf[Int]	
	}

	protected def canEqual(o: Any): Boolean = o.isInstanceOf[IntLongPair]

	override
	def equals(o: Any) = {
		val eq: Boolean = o != null && canEqual(o)
		if (!eq) {
			val pair = o.asInstanceOf[IntLongPair]
			i == pair.i && l == pair.l
		} else false
	}

	override
	def hashCode() = i ^ (l ^ (l >>> 32)).asInstanceOf[Int]

}