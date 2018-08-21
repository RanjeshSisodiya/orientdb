package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.ridbag.bonsaibucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.PageOperationTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

import java.nio.ByteBuffer;

public class OBonsaiBucketAddNonLeafEntryPageOperation implements OPageOperation {
  private int                  index;
  private byte[]               key;
  private OBonsaiBucketPointer leftChild;
  private OBonsaiBucketPointer rightChild;
  private boolean              updateNeighbours;

  public OBonsaiBucketAddNonLeafEntryPageOperation() {
  }

  public OBonsaiBucketAddNonLeafEntryPageOperation(int index, byte[] key, OBonsaiBucketPointer leftChild,
      OBonsaiBucketPointer rightChild, boolean updateNeighbours) {
    this.index = index;
    this.key = key;
    this.leftChild = leftChild;
    this.rightChild = rightChild;
    this.updateNeighbours = updateNeighbours;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    content[offset] = updateNeighbours ? (byte) 1 : (byte) 0;
    offset++;

    OIntegerSerializer.INSTANCE.serializeNative(index, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(key.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(key, 0, content, offset, key.length);
    offset += key.length;

    OLongSerializer.INSTANCE.serializeNative(leftChild.getPageIndex(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(leftChild.getPageOffset(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(rightChild.getPageIndex(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(rightChild.getPageOffset(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.put(updateNeighbours ? (byte) 1 : (byte) 0);

    buffer.putInt(index);
    buffer.putInt(key.length);
    buffer.put(key);

    buffer.putLong(leftChild.getPageIndex());
    buffer.putInt(leftChild.getPageOffset());

    buffer.putLong(rightChild.getPageIndex());
    buffer.putInt(rightChild.getPageOffset());
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    updateNeighbours = content[offset] == 1;
    offset++;

    index = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int keyLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    key = new byte[keyLen];

    System.arraycopy(content, offset, key, 0, keyLen);
    offset += keyLen;

    long leftPageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    int leftPageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    long rightPageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    int rightPageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    leftChild = new OBonsaiBucketPointer(leftPageIndex, leftPageOffset);
    rightChild = new OBonsaiBucketPointer(rightPageIndex, rightPageOffset);

    return offset;
  }

  @Override
  public int serializedSize() {
    return OByteSerializer.BYTE_SIZE + 2 * OIntegerSerializer.INT_SIZE + key.length + 2 * OLongSerializer.LONG_SIZE
        + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public byte getId() {
    return PageOperationTypes.BONSAI_BUCKET_ADD_NON_LEAF_ENTRY;
  }
}
