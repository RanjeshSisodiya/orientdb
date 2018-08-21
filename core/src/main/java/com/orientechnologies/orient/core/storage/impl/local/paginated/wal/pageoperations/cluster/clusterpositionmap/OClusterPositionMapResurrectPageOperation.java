package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.PageOperationTypes;

import java.nio.ByteBuffer;

public class OClusterPositionMapResurrectPageOperation implements OPageOperation {
  private int  index;
  private long pageIndex;
  private int  recordsOffset;

  public OClusterPositionMapResurrectPageOperation() {
  }

  public OClusterPositionMapResurrectPageOperation(int index, long pageIndex, int recordsOffset) {
    this.index = index;
    this.pageIndex = pageIndex;
    this.recordsOffset = recordsOffset;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(recordsOffset, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(index);
    buffer.putLong(pageIndex);
    buffer.putInt(recordsOffset);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    recordsOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return 2 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return PageOperationTypes.CLUSTER_POSITION_MAP_RESURRECT;
  }
}
