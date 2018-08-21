package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.PageOperationTypes;

import java.nio.ByteBuffer;

public class OClusterPageReplaceRecordPageOperation implements OPageOperation {
  private int    entryIndex;
  private byte[] record;
  private int    recordVersion;

  public OClusterPageReplaceRecordPageOperation() {
  }

  public OClusterPageReplaceRecordPageOperation(int entryIndex, byte[] record, int recordVersion) {
    this.entryIndex = entryIndex;
    this.record = record;
    this.recordVersion = recordVersion;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(entryIndex, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(record.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(record, 0, content, offset, record.length);
    offset += record.length;

    OIntegerSerializer.INSTANCE.serializeNative(recordVersion, content, offset);
    offset += OIntegerSerializer.INT_SIZE;
    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(entryIndex);
    buffer.putInt(record.length);
    buffer.put(record);
    buffer.putInt(recordVersion);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    entryIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int recordLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    record = new byte[recordLen];

    System.arraycopy(content, offset, record, 0, recordLen);
    offset += recordLen;

    recordVersion = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return 3 * OIntegerSerializer.INT_SIZE + record.length;
  }

  @Override
  public byte getId() {
    return PageOperationTypes.CLUSTER_PAGE_REPLACE_RECORD;
  }
}
