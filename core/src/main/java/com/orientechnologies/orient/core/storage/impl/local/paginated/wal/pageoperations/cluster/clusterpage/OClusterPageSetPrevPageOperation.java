package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OClusterPageSetPrevPageOperation extends OPageOperation {
  private long prevPage;

  public OClusterPageSetPrevPageOperation() {
  }

  public OClusterPageSetPrevPageOperation(long prevPage) {
    this.prevPage = prevPage;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(prevPage, content, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putLong(prevPage);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    prevPage = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int serializedSize() {
    return OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_SET_PREV_PAGE_PAGE_OPERATION;
  }
}
