package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations;

import java.nio.ByteBuffer;

public interface OPageOperation {
  int toStream(byte[] content, int offset);

  void toStream(ByteBuffer buffer);

  int fromStream(byte[] content, int offset);

  int serializedSize();

  byte getId();
}
