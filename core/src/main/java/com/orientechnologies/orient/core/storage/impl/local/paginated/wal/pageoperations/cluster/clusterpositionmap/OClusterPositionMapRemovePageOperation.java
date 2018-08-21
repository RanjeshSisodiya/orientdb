package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpositionmap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OClusterPositionMapRemovePageOperation implements OPageOperation {
  private int index;

  public OClusterPositionMapRemovePageOperation() {
  }

  public OClusterPositionMapRemovePageOperation(int index) {
    this.index = index;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    return offset + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(index);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return OIntegerSerializer.INT_SIZE;
  }

  @Override
  public byte getId() {
    return 0;
  }
}
