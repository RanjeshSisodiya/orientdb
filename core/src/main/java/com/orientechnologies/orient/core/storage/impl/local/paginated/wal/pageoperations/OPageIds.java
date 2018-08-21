package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations;

public interface OPageIds {
  byte SYS_BONSAI_BUCKET               = 1;
  byte BONSAI_BUCKET                   = 2;
  byte HASH_INDEX_DIRECTORY_PAGE       = 3;
  byte HASH_INDEX_FIRST_DIRECTORY_PAGE = 4;
  byte CLUSTER_PAGE                    = 5;
  byte HASH_INDEX_BUCKET               = 6;
  byte HASH_INDEX_NULL_BUCKET          = 7;
  byte SBTREE_BUCKET                   = 8;
  byte CLUSTER_POSITION_MAP_BUCKET     = 9;
  byte SBTREE_VALUE_PAGE               = 10;
  byte PAGINATED_CLUSTER_STATE         = 11;
  byte SBTREE_NULL_BUCKET              = 12;
  byte HASH_INDEX_FILE_LEVEL_METADATA  = 13;
}
