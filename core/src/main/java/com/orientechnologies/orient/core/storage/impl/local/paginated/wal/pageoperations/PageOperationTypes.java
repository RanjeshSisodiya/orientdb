package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.pageoperations;

public interface PageOperationTypes {
  byte SYS_BUCKET_INIT                   = 1;
  byte SYS_BUCKET_SET_FREE_LIST_LENGTH   = 2;
  byte SYS_BUCKET_SET_FREE_SPACE_POINTER = 3;
  byte SYS_BUCKET_SET_FREE_LIST_HEAD     = 4;

  byte BONSAI_BUCKET_INIT_OPERATION     = 5;
  byte BONSAI_BUCKET_SET_TREE_SIZE      = 6;
  byte BONSAI_BUCKET_REMOVE             = 7;
  byte BONSAI_BUCKET_ADD_ALL            = 8;
  byte BONSAI_BUCKET_SHRINK             = 9;
  byte BONSAI_BUCKET_ADD_LEAF_ENTRY     = 10;
  byte BONSAI_BUCKET_ADD_NON_LEAF_ENTRY = 11;
  byte BONSAI_BUCKET_UPDATE_VALUE       = 12;
  byte BONSAI_BUCKET_SET_DELETED        = 13;
  byte BONSAI_BUCKET_SET_LEFT_SIBLING   = 14;
  byte BONSAI_BUCKET_SET_RIGHT_SIBLING  = 15;

  byte CLUSTER_PAGE_INIT_OPERATION          = 16;
  byte CLUSTER_PAGE_APPEND_RECORD           = 17;
  byte CLUSTER_PAGE_REPLACE_RECORD          = 18;
  byte CLUSTER_PAGE_DELETE_RECORD           = 19;
  byte CLUSTER_PAGE_SET_NEXT_PAGE           = 20;
  byte CLUSTER_PAGE_SET_PREV_PAGE           = 21;
  byte CLUSTER_PAGE_SET_NEXT_RECORD_POINTER = 22;

  byte CLUSTER_POSITION_MAP_ADD_PAGE  = 23;
  byte CLUSTER_POSITION_MAP_ALLOCATE  = 24;
  byte CLUSTER_POSITION_MAP_SET       = 25;
  byte CLUSTER_POSITION_MAP_RESURRECT = 26;

  byte CLUSTER_STATE_SET_SIZE         = 27;
  byte CLUSTER_STATE_SET_RECORDS_SIZE = 28;

  byte SBTREE_BUCKET_INIT                = 29;
  byte SBTREE_BUCKET_SET_TREE_SIZE       = 30;
  byte SBTREE_BUCKET_SET_FREE_LIST_INDEX = 31;
  byte SBTREE_BUCKET_REMOVE              = 32;
  byte SBTREE_BUCKET_ADD_ALL             = 33;
  byte SBTREE_BUCKET_SHRINK              = 34;
  byte SBTREE_BUCKET_ADD_LEAF_ENTRY      = 35;
  byte SBTREE_BUCKET_ADD_NON_LEAF_ENTRY  = 36;
  byte SBTREE_BUCKET_UPDATE_VALUE        = 37;
  byte SBTREE_BUCKET_SET_LEFT_SIBLING    = 38;
  byte SBTREE_BUCKET_SET_RIGHT_SIBLING   = 39;

  byte SBTREE_NULL_BUCKET_INIT         = 40;
  byte SBTREE_NULL_BUCKET_SET_VALUE    = 41;
  byte SBTREE_NULL_BUCKET_REMOVE_VALUE = 42;

  static OPageOperation getInstance(int id) {
    return null;
  }
}
