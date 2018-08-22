package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.ridbag.bonsaibucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OBonsaiBucketSetDeletedPageOperation extends OPageOperation {
  private boolean deleted;

  public OBonsaiBucketSetDeletedPageOperation() {
  }

  public OBonsaiBucketSetDeletedPageOperation(boolean deleted) {
    this.deleted = deleted;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    content[offset] = deleted ? (byte) 1 : (byte) 0;
    offset++;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.put(deleted ? (byte) 1 : (byte) 0);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    deleted = content[offset] == 1;
    offset++;

    return offset;
  }

  @Override
  public int serializedSize() {
    return OByteSerializer.BYTE_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.BONSAI_BUCKET_SET_DELETED_PAGE_OPERATION;
  }
}
