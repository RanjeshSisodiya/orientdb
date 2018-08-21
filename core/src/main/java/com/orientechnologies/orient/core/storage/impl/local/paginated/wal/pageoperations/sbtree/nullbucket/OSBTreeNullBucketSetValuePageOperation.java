package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.sbtree.nullbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.PageOperationTypes;

import java.nio.ByteBuffer;

public class OSBTreeNullBucketSetValuePageOperation implements OPageOperation {
  private byte[] value;

  public OSBTreeNullBucketSetValuePageOperation() {
  }

  public OSBTreeNullBucketSetValuePageOperation(byte[] value) {
    this.value = value;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(value.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(value, 0, content, offset, value.length);
    offset += value.length;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(value.length);
    buffer.put(value);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    final int valueLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    value = new byte[valueLen];
    System.arraycopy(content, offset, value, 0, valueLen);
    offset += valueLen;

    return offset;
  }

  @Override
  public int serializedSize() {
    return OIntegerSerializer.INT_SIZE + value.length;
  }

  @Override
  public byte getId() {
    return PageOperationTypes.SBTREE_NULL_BUCKET_SET_VALUE;
  }
}
