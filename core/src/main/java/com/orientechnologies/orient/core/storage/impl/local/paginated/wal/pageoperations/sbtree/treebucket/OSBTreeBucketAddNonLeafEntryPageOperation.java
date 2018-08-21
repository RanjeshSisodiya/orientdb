package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.sbtree.treebucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.PageOperationTypes;

import java.nio.ByteBuffer;

public class OSBTreeBucketAddNonLeafEntryPageOperation implements OPageOperation {
  private int index;

  private byte[] key;

  private boolean updateNeighbours;

  private long leftChild;
  private long rightChild;

  public OSBTreeBucketAddNonLeafEntryPageOperation() {
  }

  public OSBTreeBucketAddNonLeafEntryPageOperation(int index, byte[] key, boolean updateNeighbours, long leftChild,
      long rightChild) {
    this.index = index;
    this.key = key;

    this.updateNeighbours = updateNeighbours;
    this.leftChild = leftChild;
    this.rightChild = rightChild;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(key.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(key, 0, content, offset, key.length);
    offset += key.length;

    content[offset] = updateNeighbours ? (byte) 1 : (byte) 0;
    offset++;

    OLongSerializer.INSTANCE.serializeNative(leftChild, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(rightChild, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putInt(index);

    buffer.putInt(key.length);
    buffer.put(key);

    buffer.put(updateNeighbours ? (byte) 1 : (byte) 0);

    buffer.putLong(leftChild);
    buffer.putLong(rightChild);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int keySize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    key = new byte[keySize];
    System.arraycopy(content, offset, key, 0, keySize);
    offset += keySize;

    updateNeighbours = content[offset] == 1;
    offset++;

    leftChild = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    rightChild = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int serializedSize() {
    return 2 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE + key.length + 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return PageOperationTypes.SBTREE_BUCKET_ADD_NON_LEAF_ENTRY;
  }
}
