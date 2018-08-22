package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.ridbag.bonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations.OPageOperation;

import java.nio.ByteBuffer;

public class OBonsaiBucketInitPageOperation extends OPageOperation {
  @Override
  public int toStream(byte[] content, int offset) {
    return 0;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    return 0;
  }

  @Override
  public int serializedSize() {
    return 0;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.BONSAI_BUCKET_INIT_OPERATION_PAGE_OPERATION;
  }
}
