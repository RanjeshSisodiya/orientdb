package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.ridbag.sysbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OSysBucketInitPageOperation extends OPageOperation {
  @Override
  public int toStream(byte[] content, int offset) {
    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    return offset;
  }

  @Override
  public int serializedSize() {
    return 0;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SYS_BUCKET_INIT_PAGE_OPERATION;
  }
}
