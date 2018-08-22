package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OClusterPageSetNextPageOperation extends OPageOperation {
  private long nextPage;

  public OClusterPageSetNextPageOperation() {
  }

  public OClusterPageSetNextPageOperation(long nextPage) {
    this.nextPage = nextPage;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(nextPage, content, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putLong(nextPage);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    nextPage = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int serializedSize() {
    return OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_SET_NEXT_PAGE_PAGE_OPERATION;
  }
}
