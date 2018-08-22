package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.sbtree.treebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;
import java.util.List;

public class OSBTreeBucketAddAllPageOperation extends OPageOperation {
  private int serializedSize = -1;

  private List<byte[]> entries;

  public OSBTreeBucketAddAllPageOperation() {
  }

  public OSBTreeBucketAddAllPageOperation(List<byte[]> entries) {
    this.entries = entries;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(entries.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (byte[] entry : entries) {
      OIntegerSerializer.INSTANCE.serializeNative(entry.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(entry, 0, content, offset, entry.length);
      offset += entry.length;
    }

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(entries.size());

    for (byte[] entry : entries) {
      buffer.putInt(entry.length);
      buffer.put(entry);
    }
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    final int entriesSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (int i = 0; i < entriesSize; i++) {
      final int entrySize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final byte[] entry = new byte[entriesSize];
      System.arraycopy(content, offset, entry, 0, entriesSize);
      offset += entriesSize;

      entries.add(entry);
    }

    return offset;
  }

  @Override
  public int serializedSize() {
    if (serializedSize >= 0) {
      return serializedSize;
    }

    int size = OIntegerSerializer.INT_SIZE + entries.size() * OIntegerSerializer.INT_SIZE;
    for (byte[] entry : entries) {
      size += entry.length;
    }

    serializedSize = size;

    return serializedSize;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_ADD_ALL_PAGE_OPERATION;
  }
}
