package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

@SuppressWarnings("WeakerAccess")
public final class WALRecordTypes {
  public static final byte UPDATE_PAGE_RECORD                    = 0;
  public static final byte FUZZY_CHECKPOINT_START_RECORD         = 1;
  public static final byte FUZZY_CHECKPOINT_END_RECORD           = 2;
  public static final byte FULL_CHECKPOINT_START_RECORD          = 4;
  public static final byte CHECKPOINT_END_RECORD                 = 5;
  public static final byte ATOMIC_UNIT_START_RECORD              = 8;
  public static final byte ATOMIC_UNIT_END_RECORD                = 9;
  public static final byte FILE_CREATED_WAL_RECORD               = 10;
  public static final byte NON_TX_OPERATION_PERFORMED_WAL_RECORD = 11;
  public static final byte FILE_DELETED_WAL_RECORD               = 12;
  public static final byte FILE_TRUNCATED_WAL_RECORD             = 13;
  public static final byte EMPTY_WAL_RECORD                      = 14;

  public static final byte BACKUP_PAGE_RECORD                               = 15;
  public static final byte SYS_BUCKET_INIT_PAGE_OPERATION                   = 16;
  public static final byte SYS_BUCKET_SET_FREE_LIST_LENGTH_PAGE_OPERATION   = 17;
  public static final byte SYS_BUCKET_SET_FREE_SPACE_POINTER_PAGE_OPERATION = 18;
  public static final byte SYS_BUCKET_SET_FREE_LIST_HEAD_PAGE_OPERATION     = 19;

  public static final byte BONSAI_BUCKET_INIT_OPERATION_PAGE_OPERATION     = 20;
  public static final byte BONSAI_BUCKET_SET_TREE_SIZE_PAGE_OPERATION      = 21;
  public static final byte BONSAI_BUCKET_REMOVE_PAGE_OPERATION             = 22;
  public static final byte BONSAI_BUCKET_ADD_ALL_PAGE_OPERATION            = 23;
  public static final byte BONSAI_BUCKET_SHRINK_PAGE_OPERATION             = 24;
  public static final byte BONSAI_BUCKET_ADD_LEAF_ENTRY_PAGE_OPERATION     = 25;
  public static final byte BONSAI_BUCKET_ADD_NON_LEAF_ENTRY_PAGE_OPERATION = 26;
  public static final byte BONSAI_BUCKET_UPDATE_VALUE_PAGE_OPERATION       = 27;
  public static final byte BONSAI_BUCKET_SET_DELETED_PAGE_OPERATION        = 28;
  public static final byte BONSAI_BUCKET_SET_LEFT_SIBLING_PAGE_OPERATION   = 29;
  public static final byte BONSAI_BUCKET_SET_RIGHT_SIBLING_PAGE_OPERATION  = 30;

  public static final byte CLUSTER_PAGE_INIT_OPERATION_PAGE_OPERATION          = 31;
  public static final byte CLUSTER_PAGE_APPEND_RECORD_PAGE_OPERATION           = 32;
  public static final byte CLUSTER_PAGE_REPLACE_RECORD_PAGE_OPERATION          = 33;
  public static final byte CLUSTER_PAGE_DELETE_RECORD_PAGE_OPERATION           = 34;
  public static final byte CLUSTER_PAGE_SET_NEXT_PAGE_PAGE_OPERATION           = 35;
  public static final byte CLUSTER_PAGE_SET_PREV_PAGE_PAGE_OPERATION           = 36;
  public static final byte CLUSTER_PAGE_SET_NEXT_RECORD_POINTER_PAGE_OPERATION = 37;

  public static final byte CLUSTER_POSITION_MAP_ADD_PAGE_PAGE_OPERATION  = 38;
  public static final byte CLUSTER_POSITION_MAP_ALLOCATE_PAGE_OPERATION  = 39;
  public static final byte CLUSTER_POSITION_MAP_SET_PAGE_OPERATION       = 40;
  public static final byte CLUSTER_POSITION_MAP_RESURRECT_PAGE_OPERATION = 41;

  public static final byte CLUSTER_STATE_SET_SIZE_PAGE_OPERATION         = 42;
  public static final byte CLUSTER_STATE_SET_RECORDS_SIZE_PAGE_OPERATION = 43;

  public static final byte SBTREE_BUCKET_INIT_PAGE_OPERATION                = 44;
  public static final byte SBTREE_BUCKET_SET_TREE_SIZE_PAGE_OPERATION       = 45;
  public static final byte SBTREE_BUCKET_SET_FREE_LIST_INDEX_PAGE_OPERATION = 46;
  public static final byte SBTREE_BUCKET_REMOVE_PAGE_OPERATION              = 47;
  public static final byte SBTREE_BUCKET_ADD_ALL_PAGE_OPERATION             = 48;
  public static final byte SBTREE_BUCKET_SHRINK_PAGE_OPERATION              = 49;
  public static final byte SBTREE_BUCKET_ADD_LEAF_ENTRY_PAGE_OPERATION      = 50;
  public static final byte SBTREE_BUCKET_ADD_NON_LEAF_ENTRY_PAGE_OPERATION  = 51;
  public static final byte SBTREE_BUCKET_UPDATE_VALUE_PAGE_OPERATION        = 52;
  public static final byte SBTREE_BUCKET_SET_LEFT_SIBLING_PAGE_OPERATION    = 53;
  public static final byte SBTREE_BUCKET_SET_RIGHT_SIBLING_PAGE_OPERATION   = 54;

  public static final byte SBTREE_NULL_BUCKET_INIT_PAGE_OPERATION         = 55;
  public static final byte SBTREE_NULL_BUCKET_SET_VALUE_PAGE_OPERATION    = 56;
  public static final byte SBTREE_NULL_BUCKET_REMOVE_VALUE_PAGE_OPERATION = 57;

  public static final byte BONSAI_BUCKET_SET_FREE_LIST_POINTER_PAGE_OPERATION = 58;
  public static final byte CLUSTER_STATE_SET_FREE_LIST_PAGE_OPERATION         = 59;
  public static final byte CLUSTER_POSITION_MAP_REMOVE_PAGE_OPERATION         = 60;
}
