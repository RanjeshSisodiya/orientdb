package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.ridbag.bonsaibucket;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OBonsaiBucketSetTreeSizePageOperation extends OPageOperation {
  private long size;

  public OBonsaiBucketSetTreeSizePageOperation(long size) {
    this.size = size;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(size, content, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putLong(size);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    size = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int serializedSize() {
    return OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.BONSAI_BUCKET_SET_TREE_SIZE_PAGE_OPERATION;
  }
}
