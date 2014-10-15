package com.tapad.aerospike

import com.aerospike.client.Value
import com.aerospike.client.Value.{ByteSegmentValue, BytesValue}
import io.netty.buffer.{ByteBuf, Unpooled}

/*
 * Defines a mapping to the Aerospike "Values" and from the stored object to a representation the client can work with.
 * Note that this should not be used to do deserialization but just to map to some common underlying format, since it will be applied
 * to all bins in a multi bin query and would thus potentially have to be able to handle various formats and return a generic supertype.
 * It will also be run on the selector threads, so don't do too much work here.
 */
trait ValueMapping[T] {
  def toAerospikeValue(t: T): Value
  def fromStoredObject(v: Object): T
}

object DefaultValueMappings {
  implicit val byteArrayMapping = new ValueMapping[Array[Byte]] {
    def toAerospikeValue(arr: Array[Byte]) = new BytesValue(arr)
    def fromStoredObject(v: Object): Array[Byte] = v.asInstanceOf[Array[Byte]]
  }
  implicit val byteBufMapping = new ValueMapping[ByteBuf] {
    def toAerospikeValue(buf: ByteBuf) = new ByteSegmentValue(buf.array, buf.arrayOffset + buf.readerIndex, buf.readableBytes)
    def fromStoredObject(v: Object): ByteBuf = Unpooled.wrappedBuffer(v.asInstanceOf[Array[Byte]])
  }
}
