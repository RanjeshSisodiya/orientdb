package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.sbtree.treebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OSBTreeBucketAddLeafEntryPageOperation extends OPageOperation {
  private int    index;
  private byte[] key;
  private byte[] value;

  public OSBTreeBucketAddLeafEntryPageOperation() {
  }

  public OSBTreeBucketAddLeafEntryPageOperation(int index, byte[] key, byte[] value) {
    this.index = index;
    this.key = key;
    this.value = value;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(key.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(key, 0, content, offset, key.length);
    offset += key.length;

    OIntegerSerializer.INSTANCE.serializeNative(value.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(value, 0, content, offset, value.length);
    offset += value.length;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(index);

    buffer.putInt(key.length);
    buffer.put(key);

    buffer.putInt(value.length);
    buffer.put(value);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int keyLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    key = new byte[keyLen];
    System.arraycopy(content, offset, key, 0, keyLen);
    offset += keyLen;

    int valueLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    value = new byte[valueLen];
    System.arraycopy(content, offset, value, 0, valueLen);
    offset += valueLen;

    return offset;
  }

  @Override
  public int serializedSize() {
    return 3 * OIntegerSerializer.INT_SIZE + value.length + key.length;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_ADD_LEAF_ENTRY_PAGE_OPERATION;
  }
}
