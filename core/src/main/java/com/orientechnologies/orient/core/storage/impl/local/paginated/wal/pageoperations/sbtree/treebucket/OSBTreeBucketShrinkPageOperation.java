package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.sbtree.treebucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OSBTreeBucketShrinkPageOperation extends OPageOperation {
  private int     newSize;
  private boolean isEncrypted;

  public OSBTreeBucketShrinkPageOperation() {
  }

  public OSBTreeBucketShrinkPageOperation(int newSize, boolean isEncrypted) {
    this.newSize = newSize;
    this.isEncrypted = isEncrypted;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(newSize, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    content[offset] = isEncrypted ? (byte) 1 : (byte) 0;
    offset++;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(newSize);
    buffer.put(isEncrypted ? (byte) 1 : (byte) 0);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    newSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    content[offset] = isEncrypted ? (byte) 1 : (byte) 0;

    return offset;
  }

  @Override
  public int serializedSize() {
    return OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_SHRINK_PAGE_OPERATION;
  }
}
