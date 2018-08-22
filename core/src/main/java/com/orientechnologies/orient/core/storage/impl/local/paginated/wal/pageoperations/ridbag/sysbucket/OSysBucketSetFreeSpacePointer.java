package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.ridbag.sysbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

import java.nio.ByteBuffer;

public class OSysBucketSetFreeSpacePointer extends OPageOperation {
  private OBonsaiBucketPointer pointer;

  public OSysBucketSetFreeSpacePointer(OBonsaiBucketPointer pointer) {
    this.pointer = pointer;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(pointer.getPageIndex(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(pointer.getPageOffset(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putLong(pointer.getPageIndex());
    buffer.putInt(pointer.getPageOffset());
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    final long pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    final int pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    pointer = new OBonsaiBucketPointer(pageIndex, pageOffset);

    return offset;
  }

  @Override
  public int serializedSize() {
    return OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SYS_BUCKET_SET_FREE_SPACE_POINTER_PAGE_OPERATION;
  }
}
