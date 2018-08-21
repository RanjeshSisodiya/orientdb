package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.sbtree.treebucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.PageOperationTypes;

import java.nio.ByteBuffer;

public class OSBTreeBucketUpdateValuePageOperation implements OPageOperation {
  private boolean encrypted;
  private int     index;
  private byte[]  value;

  public OSBTreeBucketUpdateValuePageOperation() {
  }

  public OSBTreeBucketUpdateValuePageOperation(boolean encrypted, int index, byte[] value) {
    this.encrypted = encrypted;
    this.index = index;
    this.value = value;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    content[offset] = encrypted ? (byte) 1 : (byte) 0;
    offset++;

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(value.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(value, 0, content, offset, value.length);
    offset += value.length;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.put(encrypted ? (byte) 1 : (byte) 0);
    buffer.putInt(index);

    buffer.putInt(value.length);
    buffer.put(value);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    encrypted = content[offset] == 1;
    offset++;

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final int valLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    value = new byte[valLen];
    System.arraycopy(content, offset, value, 0, valLen);
    offset += valLen;

    return offset;
  }

  @Override
  public int serializedSize() {
    return OByteSerializer.BYTE_SIZE + 2 * OIntegerSerializer.INT_SIZE + value.length;
  }

  @Override
  public byte getId() {
    return PageOperationTypes.SBTREE_BUCKET_UPDATE_VALUE;
  }
}
