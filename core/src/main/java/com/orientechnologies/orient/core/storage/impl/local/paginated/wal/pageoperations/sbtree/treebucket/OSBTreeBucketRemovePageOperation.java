package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.sbtree.treebucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OSBTreeBucketRemovePageOperation extends OPageOperation {
  private int     entryIndex;
  private boolean isEncrypted;

  public OSBTreeBucketRemovePageOperation() {
  }

  public OSBTreeBucketRemovePageOperation(int entryIndex, boolean isEncrypted) {
    this.entryIndex = entryIndex;
    this.isEncrypted = isEncrypted;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(entryIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    content[offset] = isEncrypted ? (byte) 1 : (byte) 0;
    offset++;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(entryIndex);
    buffer.put(isEncrypted ? (byte) 1 : (byte) 0);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    entryIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    isEncrypted = content[offset] == 1;
    offset++;

    return offset;
  }

  @Override
  public int serializedSize() {
    return OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_REMOVE_PAGE_OPERATION;
  }
}
