package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OClusterPageAppendRecordPageOperation extends OPageOperation {
  private int    recordVersion;
  private byte[] record;

  public OClusterPageAppendRecordPageOperation() {
  }

  public OClusterPageAppendRecordPageOperation(int recordVersion, byte[] record) {
    this.recordVersion = recordVersion;
    this.record = record;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(recordVersion, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(record.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(record, 0, content, offset, record.length);
    offset += record.length;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(recordVersion);
    buffer.putInt(record.length);
    buffer.put(record);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    recordVersion = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int recordLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    record = new byte[recordLen];
    System.arraycopy(content, offset, record, 0, recordLen);
    offset += recordLen;

    return offset;
  }

  @Override
  public int serializedSize() {
    return 2 * OIntegerSerializer.INT_SIZE + record.length;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_APPEND_RECORD_PAGE_OPERATION;
  }
}
