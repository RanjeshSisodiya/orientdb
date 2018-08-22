package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OClusterPageDeleteRecordPageOperation extends OPageOperation {
  private int position;

  public OClusterPageDeleteRecordPageOperation() {
  }

  public OClusterPageDeleteRecordPageOperation(int position) {
    this.position = position;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(position, content, offset);
    return offset + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(position);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    position = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    return offset + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int serializedSize() {
    return OIntegerSerializer.INT_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_DELETE_RECORD_PAGE_OPERATION;
  }
}
