package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.sbtree.treebucket;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OSBTreeBucketSetRightSiblingPageOperation extends OPageOperation {
  private long pageIndex;

  public OSBTreeBucketSetRightSiblingPageOperation() {
  }

  public OSBTreeBucketSetRightSiblingPageOperation(long pageIndex) {
    this.pageIndex = pageIndex;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putLong(pageIndex);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int serializedSize() {
    return OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_SET_RIGHT_SIBLING_PAGE_OPERATION;
  }
}
