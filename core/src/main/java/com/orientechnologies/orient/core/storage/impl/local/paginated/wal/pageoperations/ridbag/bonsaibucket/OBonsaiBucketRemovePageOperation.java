package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.ridbag.bonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OBonsaiBucketRemovePageOperation extends OPageOperation {
  private int entryIndex;

  public OBonsaiBucketRemovePageOperation(int entryIndex) {
    this.entryIndex = entryIndex;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(entryIndex, content, offset);
    return offset + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(entryIndex);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    entryIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return OIntegerSerializer.INT_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.BONSAI_BUCKET_REMOVE_PAGE_OPERATION;
  }
}
