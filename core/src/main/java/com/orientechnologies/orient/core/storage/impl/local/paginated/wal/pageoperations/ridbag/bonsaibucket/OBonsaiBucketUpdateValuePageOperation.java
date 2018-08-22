package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.ridbag.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OBonsaiBucketUpdateValuePageOperation extends OPageOperation {
  private int    index;
  private byte[] value;

  public OBonsaiBucketUpdateValuePageOperation() {
  }

  public OBonsaiBucketUpdateValuePageOperation(int index, byte[] value) {
    this.index = index;
    this.value = value;
  }

  @Override
  public int toStream(byte[] content, int offset) {
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
    buffer.putInt(index);
    buffer.putInt(value.length);
    buffer.put(value);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int valueLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    value = new byte[valueLen];
    System.arraycopy(content, offset, value, 0, valueLen);
    offset += valueLen;

    return offset;
  }

  @Override
  public int serializedSize() {
    return 2 * OIntegerSerializer.INT_SIZE + value.length;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.BONSAI_BUCKET_UPDATE_VALUE_PAGE_OPERATION;
  }
}
