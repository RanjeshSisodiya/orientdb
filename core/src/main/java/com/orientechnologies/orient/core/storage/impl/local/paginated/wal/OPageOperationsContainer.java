package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.PageOperationTypes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class OPageOperationsContainer extends OAbstractPageWALRecord {
  private byte                 walId;
  private List<OPageOperation> pageOperations;

  private int serializedSize = -1;

  public OPageOperationsContainer() {
  }

  public OPageOperationsContainer(long pageIndex, long fileId, OOperationUnitId operationUnitId,
      List<OPageOperation> pageOperations, byte walId) {
    super(pageIndex, fileId, operationUnitId);

    this.pageOperations = pageOperations;
    this.walId = walId;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.PAGE_OPERATIONS_CONTAINER;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    content[offset] = walId;
    offset++;

    OIntegerSerializer.INSTANCE.serializeLiteral(pageOperations.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (OPageOperation pageOperation : pageOperations) {
      content[offset] = pageOperation.getId();
      offset++;

      offset = pageOperation.toStream(content, offset);
    }

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.put(walId);
    buffer.putInt(pageOperations.size());

    for (OPageOperation pageOperation : pageOperations) {
      buffer.put(pageOperation.getId());
      pageOperation.toStream(buffer);
    }
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    walId = content[offset];
    offset++;

    final int operations = OIntegerSerializer.INSTANCE.deserializeLiteral(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    pageOperations = new ArrayList<>();

    for (int i = 0; i < operations; i++) {
      final int pageOperationId = content[offset];
      offset++;

      final OPageOperation pageOperation = PageOperationTypes.getInstance(pageOperationId);
      pageOperations.add(pageOperation);
    }

    return offset;
  }

  @Override
  public int serializedSize() {
    if (serializedSize >= 0) {
      return serializedSize;
    }

    int size = super.serializedSize();
    size += OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;

    size += pageOperations.size();
    for (OPageOperation pageOperation : pageOperations) {
      size += pageOperation.serializedSize();
    }

    serializedSize = size;

    return serializedSize;
  }
}
