package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class OBackupPageRecord extends OAbstractWALRecord {
  private boolean newPage;
  private byte[]  page;

  private long pageIndex;
  private long fileId;

  public OBackupPageRecord() {
  }

  public OBackupPageRecord(long pageIndex, long fileId, boolean newPage, byte[] page) {
    this.newPage = newPage;
    this.page = page;
    this.pageIndex = pageIndex;
    this.fileId = fileId;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.BACKUP_PAGE_RECORD;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    OLongSerializer.INSTANCE.serializeNative(fileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(pageIndex, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    if (newPage) {
      content[offset] = 1;
      offset++;
    } else {
      offset++;

      OIntegerSerializer.INSTANCE.serializeNative(page.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(page, 0, content, offset, page.length);
      offset += page.length;
    }

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    buffer.putLong(fileId);
    buffer.putLong(pageIndex);

    if (newPage) {
      buffer.put((byte) 1);
    } else {
      buffer.put((byte) 0);
      buffer.putInt(page.length);

      buffer.put(page);
    }
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    fileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    pageIndex = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    this.newPage = content[offset] == 1;
    offset++;

    if (newPage) {
      return offset;
    }

    final int pageSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    page = new byte[pageSize];
    System.arraycopy(content, offset, page, 0, pageSize);
    offset += pageSize;

    return offset;
  }

  @Override
  public int serializedSize() {
    int size = 2 * OLongSerializer.LONG_SIZE;

    if (newPage) {
      size++;
    } else {
      size++;
      size += OIntegerSerializer.INT_SIZE;
      size += page.length;
    }

    return size;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    OBackupPageRecord that = (OBackupPageRecord) o;
    return newPage == that.newPage && Arrays.equals(page, that.page);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(super.hashCode(), newPage);
    result = 31 * result + Arrays.hashCode(page);
    return result;
  }
}
