package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterstate;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OClusterStateSetSizePageOperation extends OPageOperation {
  private long size;

  public OClusterStateSetSizePageOperation() {
  }

  public OClusterStateSetSizePageOperation(long size) {
    this.size = size;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(size, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putLong(size);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    size = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_STATE_SET_SIZE_PAGE_OPERATION;
  }
}
