package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.ridbag.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OBonsaiBucketShrinkPageOperation extends OPageOperation {
  private int newSize;

  public OBonsaiBucketShrinkPageOperation(int newSize) {
    this.newSize = newSize;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(newSize, content, offset);
    return offset + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(newSize);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    newSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    return offset + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int serializedSize() {
    return OIntegerSerializer.INT_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.BONSAI_BUCKET_SHRINK_PAGE_OPERATION;
  }
}
