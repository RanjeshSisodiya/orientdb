package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractPageWALRecord;

public abstract class OPageOperation extends OAbstractPageWALRecord {
  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }
}
