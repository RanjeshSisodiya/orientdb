package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 20.03.13
 */
public class ClusterPageTest {
  @Test
  public void testAddOneRecord() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      addOneRecord(directLocalPage, bufferPool);

    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void addOneRecord(OClusterPage localPage, OByteBufferPool bufferPool) {
    int freeSpace = localPage.getFreeSpace();
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 1;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    assertAddOneRecordState(localPage, freeSpace, recordVersion, position);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      final OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertAddOneRecordState(restoredPage, freeSpace, recordVersion, position);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }

  }

  private void assertAddOneRecordState(OClusterPage localPage, int freeSpace, int recordVersion, int position) {
    Assert.assertEquals(localPage.getRecordsCount(), 1);
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(position, 0);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (27 + ORecordVersionHelper.SERIALIZED_SIZE));
    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    assertThat(localPage.getRecordBinaryValue(0, 11)).isEqualTo(new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
  }

  @Test
  public void testAddThreeRecords() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      addThreeRecords(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void addThreeRecords(OClusterPage localPage, OByteBufferPool bufferPool) {
    int freeSpace = localPage.getFreeSpace();

    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int positionOne = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int positionTwo = localPage.appendRecord(recordVersion, new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    int positionThree = localPage.appendRecord(recordVersion, new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    assertAddThreeRecordsState(localPage, freeSpace, recordVersion, positionOne, positionTwo, positionThree);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertAddThreeRecordsState(restoredPage, freeSpace, recordVersion, positionOne, positionTwo, positionThree);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertAddThreeRecordsState(OClusterPage localPage, int freeSpace, int recordVersion, int positionOne,
      int positionTwo, int positionThree) {
    Assert.assertEquals(localPage.getRecordsCount(), 3);
    Assert.assertEquals(positionOne, 0);
    Assert.assertEquals(positionTwo, 1);
    Assert.assertEquals(positionThree, 2);

    Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (3 * (27 + ORecordVersionHelper.SERIALIZED_SIZE)));
    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertFalse(localPage.isDeleted(1));
    Assert.assertFalse(localPage.isDeleted(2));

    assertThat(localPage.getRecordBinaryValue(0, 11)).isEqualTo(new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    assertThat(localPage.getRecordBinaryValue(1, 11)).isEqualTo(new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(1), recordVersion);

    assertThat(localPage.getRecordBinaryValue(2, 11)).isEqualTo(new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(2), recordVersion);
  }

  @Test
  public void testAddFullPage() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      addFullPage(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void addFullPage(OClusterPage localPage, OByteBufferPool bufferPool) {
    int recordVersion = 0;
    recordVersion++;

    List<Integer> positions = new ArrayList<>();
    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();
    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    assertAddFullPageState(localPage, recordVersion, positions);

    final ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertAddFullPageState(restoredPage, recordVersion, positions);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertAddFullPageState(OClusterPage localPage, int recordVersion, List<Integer> positions) {
    byte counter;
    Assert.assertEquals(localPage.getRecordsCount(), positions.size());

    counter = 0;
    for (int position : positions) {
      assertThat(localPage.getRecordBinaryValue(position, 3)).isEqualTo(new byte[] { counter, counter, counter });
      Assert.assertEquals(localPage.getRecordSize(position), 3);
      Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
      counter++;
    }
  }

  @Test
  public void testDeleteAddLowerVersion() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      deleteAddLowerVersion(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteAddLowerVersion(OClusterPage localPage, OByteBufferPool bufferPool) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    int newRecordVersion = 0;

    Assert.assertEquals(localPage.appendRecord(newRecordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    assertDeleteAddLowerVersionState(localPage, position, newRecordVersion);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertDeleteAddLowerVersionState(restoredPage, position, newRecordVersion);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertDeleteAddLowerVersionState(OClusterPage localPage, int position, int newRecordVersion) {
    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);
    assertThat(localPage.getRecordBinaryValue(position, recordSize)).isEqualTo(new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  @Test
  public void testDeleteAddBiggerVersion() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      deleteAddBiggerVersion(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteAddBiggerVersion(OClusterPage localPage, OByteBufferPool bufferPool) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    int newRecordVersion = 0;
    newRecordVersion++;
    newRecordVersion++;
    newRecordVersion++;
    newRecordVersion++;

    Assert.assertEquals(localPage.appendRecord(newRecordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    assertDeleteAddBiggerVersionState(localPage, position, newRecordVersion);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertDeleteAddBiggerVersionState(restoredPage, position, newRecordVersion);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertDeleteAddBiggerVersionState(OClusterPage localPage, int position, int newRecordVersion) {
    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);

    assertThat(localPage.getRecordBinaryValue(position, recordSize)).isEqualTo(new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  @Test
  public void testDeleteAddEqualVersion() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      deleteAddEqualVersion(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteAddEqualVersion(OClusterPage localPage, OByteBufferPool bufferPool) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    Assert.assertEquals(localPage.appendRecord(recordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    assertDeleteAddEqualVersionState(localPage, recordVersion, position);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertDeleteAddEqualVersionState(restoredPage, recordVersion, position);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertDeleteAddEqualVersionState(OClusterPage localPage, int recordVersion, int position) {
    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
    assertThat(localPage.getRecordBinaryValue(position, recordSize)).isEqualTo(new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  @Test
  public void testDeleteAddEqualVersionKeepTombstoneVersion() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      deleteAddEqualVersionKeepTombstoneVersion(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteAddEqualVersionKeepTombstoneVersion(OClusterPage localPage, OByteBufferPool bufferPool) {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    Assert.assertEquals(localPage.appendRecord(recordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    assertDeleteAddEqualVersionKeepTombstoneVersionState(localPage, recordVersion, position);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertDeleteAddEqualVersionKeepTombstoneVersionState(restoredPage, recordVersion, position);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertDeleteAddEqualVersionKeepTombstoneVersionState(OClusterPage localPage, int recordVersion, int position) {
    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
    assertThat(localPage.getRecordBinaryValue(position, recordSize)).isEqualTo(new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  @Test
  public void testDeleteTwoOutOfFour() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      deleteTwoOutOfFour(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteTwoOutOfFour(OClusterPage localPage, OByteBufferPool bufferPool) {
    int recordVersion = 0;
    recordVersion++;

    int positionOne = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int positionTwo = localPage.appendRecord(recordVersion, new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    int positionThree = localPage.appendRecord(recordVersion, new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
    int positionFour = localPage.appendRecord(recordVersion, new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });

    Assert.assertEquals(localPage.getRecordsCount(), 4);
    Assert.assertEquals(positionOne, 0);
    Assert.assertEquals(positionTwo, 1);
    Assert.assertEquals(positionThree, 2);
    Assert.assertEquals(positionFour, 3);

    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertFalse(localPage.isDeleted(1));
    Assert.assertFalse(localPage.isDeleted(2));
    Assert.assertFalse(localPage.isDeleted(3));

    int freeSpace = localPage.getFreeSpace();

    Assert.assertTrue(localPage.deleteRecord(0));
    Assert.assertTrue(localPage.deleteRecord(2));

    Assert.assertFalse(localPage.deleteRecord(0));
    Assert.assertFalse(localPage.deleteRecord(7));

    assertDeleteTwoOutOfFourState(localPage, recordVersion, freeSpace);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertDeleteTwoOutOfFourState(restoredPage, recordVersion, freeSpace);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertDeleteTwoOutOfFourState(OClusterPage localPage, int recordVersion, int freeSpace) {
    Assert.assertEquals(localPage.findFirstDeletedRecord(0), 0);
    Assert.assertEquals(localPage.findFirstDeletedRecord(1), 2);
    Assert.assertEquals(localPage.findFirstDeletedRecord(3), -1);

    Assert.assertTrue(localPage.isDeleted(0));
    Assert.assertEquals(localPage.getRecordSize(0), -1);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    assertThat(localPage.getRecordBinaryValue(1, 11)).isEqualTo(new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    Assert.assertEquals(localPage.getRecordSize(1), 11);
    Assert.assertEquals(localPage.getRecordVersion(1), recordVersion);

    Assert.assertTrue(localPage.isDeleted(2));
    Assert.assertEquals(localPage.getRecordSize(2), -1);
    Assert.assertEquals(localPage.getRecordVersion(2), recordVersion);

    assertThat(localPage.getRecordBinaryValue(3, 11)).isEqualTo(new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });

    Assert.assertEquals(localPage.getRecordSize(3), 11);
    Assert.assertEquals(localPage.getRecordVersion(3), recordVersion);

    Assert.assertEquals(localPage.getRecordsCount(), 2);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace + 23 * 2);
  }

  @Test
  public void testAddFullPageDeleteAndAddAgain() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      addFullPageDeleteAndAddAgain(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void addFullPageDeleteAndAddAgain(OClusterPage localPage, OByteBufferPool bufferPool) {
    Map<Integer, Byte> positionCounter = new HashMap<>();
    Set<Integer> deletedPositions = new HashSet<>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();
    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positionCounter.size());
        positionCounter.put(lastPosition, counter);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positionCounter.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i += 2) {
      localPage.deleteRecord(i);
      deletedPositions.add(i);
      positionCounter.remove(i);
    }

    freeSpace = localPage.getFreeSpace();
    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        positionCounter.put(lastPosition, counter);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 15);
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    assertAddFullPageDeleteAndAddAgainState(localPage, positionCounter, deletedPositions, recordVersion, filledRecordsCount);

    final ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertAddFullPageDeleteAndAddAgainState(restoredPage, positionCounter, deletedPositions, recordVersion, filledRecordsCount);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertAddFullPageDeleteAndAddAgainState(OClusterPage localPage, Map<Integer, Byte> positionCounter,
      Set<Integer> deletedPositions, int recordVersion, int filledRecordsCount) {
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);
    for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
      assertThat(localPage.getRecordBinaryValue(entry.getKey(), 3))
          .isEqualTo(new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });

      Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);

      if (deletedPositions.contains(entry.getKey()))
        Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), recordVersion);

    }
  }

  @Test
  public void testAddBigRecordDeleteAndAddSmallRecords() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      final long seed = System.currentTimeMillis();

      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      addBigRecordDeleteAndAddSmallRecords(seed, directLocalPage, bufferPool);

    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void addBigRecordDeleteAndAddSmallRecords(long seed, OClusterPage localPage, OByteBufferPool bufferPool) {
    final Random mersenneTwisterFast = new Random(seed);

    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    final byte[] bigChunk = new byte[OClusterPage.MAX_ENTRY_SIZE / 2];

    mersenneTwisterFast.nextBytes(bigChunk);

    int position = localPage.appendRecord(recordVersion, bigChunk);
    Assert.assertEquals(position, 0);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    Assert.assertTrue(localPage.deleteRecord(0));

    recordVersion++;
    int freeSpace = localPage.getFreeSpace();
    Map<Integer, Byte> positionCounter = new HashMap<>();
    int lastPosition;
    byte counter = 0;
    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positionCounter.size());
        positionCounter.put(lastPosition, counter);
        counter++;

        if (lastPosition == 0)
          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 15);
        else
          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));

        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    assertAddBigRecordDeleteAndAddSmallRecordsState(localPage, recordVersion, positionCounter);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertAddBigRecordDeleteAndAddSmallRecordsState(restoredPage, recordVersion, positionCounter);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertAddBigRecordDeleteAndAddSmallRecordsState(OClusterPage localPage, int recordVersion,
      Map<Integer, Byte> positionCounter) {
    Assert.assertEquals(localPage.getRecordsCount(), positionCounter.size());
    for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
      assertThat(localPage.getRecordBinaryValue(entry.getKey(), 3))
          .isEqualTo(new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });
      Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);
      Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), recordVersion);
    }
  }

  @Test
  public void testFindFirstRecord() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    final long seed = System.currentTimeMillis();
    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      findFirstRecord(seed, directLocalPage, bufferPool);

    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void findFirstRecord(long seed, OClusterPage localPage, OByteBufferPool bufferPool) {
    final Random mersenneTwister = new Random(seed);
    Set<Integer> positions = new HashSet<>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();

    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positions.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i++) {
      if (mersenneTwister.nextBoolean()) {
        localPage.deleteRecord(i);
        positions.remove(i);
      }
    }

    assertFindFirstRecordState(localPage, positions);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertFindFirstRecordState(restoredPage, positions);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertFindFirstRecordState(OClusterPage localPage, Set<Integer> positions) {
    int recordsIterated = 0;
    int recordPosition = 0;
    int lastRecordPosition = -1;

    do {
      recordPosition = localPage.findFirstRecord(recordPosition);
      if (recordPosition < 0)
        break;

      Assert.assertTrue(positions.contains(recordPosition));
      Assert.assertTrue(recordPosition > lastRecordPosition);

      lastRecordPosition = recordPosition;

      recordPosition++;
      recordsIterated++;
    } while (recordPosition >= 0);

    Assert.assertEquals(recordsIterated, positions.size());
  }

  @Test
  public void testFindLastRecord() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    final long seed = System.currentTimeMillis();
    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      findLastRecord(seed, directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();

      directCachePointer.decrementReferrer();
    }
  }

  private void findLastRecord(long seed, OClusterPage localPage, OByteBufferPool bufferPool) {
    final Random mersenneTwister = new Random(seed);
    Set<Integer> positions = new HashSet<>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();

    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positions.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i++) {
      if (mersenneTwister.nextBoolean()) {
        localPage.deleteRecord(i);
        positions.remove(i);
      }
    }

    assertFindLastRecordState(localPage, positions);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, true);
      restoredPage.deserializePage(buffer.array());

      assertFindLastRecordState(restoredPage, positions);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertFindLastRecordState(OClusterPage localPage, Set<Integer> positions) {
    int recordsIterated = 0;
    int recordPosition = Integer.MAX_VALUE;
    int lastRecordPosition = Integer.MAX_VALUE;
    do {
      recordPosition = localPage.findLastRecord(recordPosition);
      if (recordPosition < 0)
        break;

      Assert.assertTrue(positions.contains(recordPosition));
      Assert.assertTrue(recordPosition < lastRecordPosition);

      recordPosition--;
      recordsIterated++;
    } while (recordPosition >= 0);

    Assert.assertEquals(recordsIterated, positions.size());
  }

  @Test
  public void testSetGetNextPage() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      setGetNextPage(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void setGetNextPage(OClusterPage localPage, OByteBufferPool bufferPool) {
    localPage.setNextPage(1034);
    Assert.assertEquals(localPage.getNextPage(), 1034);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      Assert.assertEquals(restoredPage.getNextPage(), 1034);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  @Test
  public void testSetGetPrevPage() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      setGetPrevPage(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void setGetPrevPage(OClusterPage localPage, OByteBufferPool bufferPool) {
    localPage.setPrevPage(1034);
    Assert.assertEquals(localPage.getPrevPage(), 1034);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      Assert.assertEquals(restoredPage.getPrevPage(), 1034);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  @Test
  public void testReplaceOneRecordWithBiggerSize() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      replaceOneRecordWithBiggerSize(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordWithBiggerSize(OClusterPage localPage, OByteBufferPool bufferPool) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = recordVersion;
    newRecordVersion++;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1, 3 }, newRecordVersion);
    assertReplaceOneRecordWithBiggerSizeState(localPage, index, freeSpace, newRecordVersion, written);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertReplaceOneRecordWithBiggerSizeState(restoredPage, index, freeSpace, newRecordVersion, written);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertReplaceOneRecordWithBiggerSizeState(OClusterPage localPage, int index, int freeSpace, int newRecordVersion,
      int written) {
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);
    assertThat(localPage.getRecordBinaryValue(index, 11)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordVersion(index), newRecordVersion);
  }

  @Test
  public void testReplaceOneRecordWithEqualSize() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);

      replaceOneRecordWithEqualSize(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordWithEqualSize(OClusterPage localPage, OByteBufferPool bufferPool) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = recordVersion;
    newRecordVersion++;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 }, newRecordVersion);
    assertReplaceOneRecordWithEqualSizeState(localPage, index, freeSpace, newRecordVersion, written);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertReplaceOneRecordWithEqualSizeState(restoredPage, index, freeSpace, newRecordVersion, written);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertReplaceOneRecordWithEqualSizeState(OClusterPage localPage, int index, int freeSpace, int newRecordVersion,
      int written) {
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);
    assertThat(localPage.getRecordBinaryValue(index, 11)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordVersion(index), newRecordVersion);
  }

  @Test
  public void testReplaceOneRecordWithSmallerSize() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      replaceOneRecordWithSmallerSize(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordWithSmallerSize(OClusterPage localPage, OByteBufferPool bufferPool) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = recordVersion;
    newRecordVersion++;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, }, newRecordVersion);
    assertReplaceOneRecordWithSmallerSizeState(localPage, index, freeSpace, newRecordVersion, written);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();

    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertReplaceOneRecordWithSmallerSizeState(restoredPage, index, freeSpace, newRecordVersion, written);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertReplaceOneRecordWithSmallerSizeState(OClusterPage localPage, int index, int freeSpace, int newRecordVersion,
      int written) {
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 6);

    Assert.assertEquals(localPage.getRecordSize(index), 6);
    assertThat(localPage.getRecordBinaryValue(index, 6)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11 });
    Assert.assertEquals(localPage.getRecordVersion(index), newRecordVersion);
  }

  @Test
  public void testReplaceOneRecordNoVersionUpdate() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      replaceOneRecordNoVersionUpdate(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordNoVersionUpdate(OClusterPage localPage, OByteBufferPool bufferPool) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1, 3 }, -1);
    assertReplaceOneRecordNoVersionUpdateState(localPage, recordVersion, index, freeSpace, written);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertReplaceOneRecordNoVersionUpdateState(restoredPage, recordVersion, index, freeSpace, written);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertReplaceOneRecordNoVersionUpdateState(OClusterPage localPage, int recordVersion, int index, int freeSpace,
      int written) {
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    assertThat(localPage.getRecordBinaryValue(index, 11)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordVersion(index), recordVersion);
  }

  @Test
  public void testReplaceOneRecordLowerVersion() {
    OByteBufferPool bufferPool = OByteBufferPool.instance();
    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true);
      replaceOneRecordLowerVersion(directLocalPage, bufferPool);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordLowerVersion(OClusterPage localPage, OByteBufferPool bufferPool) {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = recordVersion;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1, 3 }, newRecordVersion);
    assertReplaceOneRecordLowerVersionState(localPage, recordVersion, index, freeSpace, written);

    ByteBuffer buffer = ByteBuffer.allocate(localPage.serializedSize()).order(ByteOrder.nativeOrder());
    localPage.serializePage(buffer);

    ByteBuffer directBuffer = bufferPool.acquireDirect(true);
    OCachePointer directCachePointer = new OCachePointer(directBuffer, bufferPool, 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    directCacheEntry.acquireExclusiveLock();
    try {
      OClusterPage restoredPage = new OClusterPage(directCacheEntry, false);
      restoredPage.deserializePage(buffer.array());

      assertReplaceOneRecordLowerVersionState(restoredPage, recordVersion, index, freeSpace, written);
    } finally {
      directCacheEntry.releaseExclusiveLock();
      directCachePointer.decrementReferrer();
    }
  }

  private void assertReplaceOneRecordLowerVersionState(OClusterPage localPage, int recordVersion, int index, int freeSpace,
      int written) {
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    assertThat(localPage.getRecordBinaryValue(index, 11)).isEqualTo(new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordVersion(index), recordVersion);
  }

}
