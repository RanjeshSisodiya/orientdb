package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.ridbag.sysbucket;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OSysBucketSetFreeListLengthPageOperation extends OPageOperation {
  private long length;

  public OSysBucketSetFreeListLengthPageOperation(long length) {
    this.length = length;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(length, content, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putLong(length);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    length = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int serializedSize() {
    return OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SYS_BUCKET_SET_FREE_LIST_LENGTH_PAGE_OPERATION;
  }
}
