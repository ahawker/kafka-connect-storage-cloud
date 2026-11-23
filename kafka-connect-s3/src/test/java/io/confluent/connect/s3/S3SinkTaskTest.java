/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.connect.s3.format.avro.AvroUtils;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.util.FileUtils;
import io.confluent.connect.storage.StorageFactory;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import io.confluent.connect.storage.partitioner.PartitionerConfig;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

@RunWith(PowerMockRunner.class)
@PrepareForTest({S3SinkTask.class, StorageFactory.class})
@PowerMockIgnore({"io.findify.s3mock.*", "akka.*", "javax.*", "org.xml.*", "com.sun.org.apache.xerces.*"})
public class S3SinkTaskTest extends DataWriterAvroTest {

  //@Before should be omitted in order to be able to add properties per test.
  public void setUp() throws Exception {
    super.setUp();
    Capture<Class<S3Storage>> capturedStorage = EasyMock.newCapture();
    Capture<Class<S3SinkConnectorConfig>> capturedStorageConf = EasyMock.newCapture();
    Capture<S3SinkConnectorConfig> capturedConf = EasyMock.newCapture();
    Capture<String> capturedUrl = EasyMock.newCapture();
    PowerMock.mockStatic(StorageFactory.class);
    EasyMock.expect(StorageFactory.createStorage(EasyMock.capture(capturedStorage),
                                                 EasyMock.capture(capturedStorageConf),
                                                 EasyMock.capture(capturedConf),
                                                 EasyMock.capture(capturedUrl))).andReturn(storage);
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    localProps.clear();
  }

  @Test
  public void testTaskType() throws Exception {
    setUp();
    replayAll();
    task = new S3SinkTask();
    SinkTask.class.isAssignableFrom(task.getClass());
  }

  @Test
  public void testWriteRecord() throws Exception {
    setUp();
    replayAll();
    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    List<SinkRecord> sinkRecords = createRecords(7);
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testWriteNullRecords() throws Exception {
    setUp();
    replayAll();
    task = new S3SinkTask();
    SinkTaskContext mockContext = mock(SinkTaskContext.class);
    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    when(mockContext.errantRecordReporter()).thenReturn(reporter);
    when(mockContext.assignment()).thenReturn(Collections.singleton(TOPIC_PARTITION));
    task.initialize(mockContext);

    properties.put(S3SinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
        S3SinkConnectorConfig.IgnoreOrFailBehavior.IGNORE.toString());
    task.start(properties);
    verifyAll();

    List<SinkRecord> sinkRecords = createRecordsWithPrimitive(3, 0,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
    sinkRecords.add(
        new SinkRecord(TOPIC, PARTITION, null, null, Schema.OPTIONAL_STRING_SCHEMA, null,
            0));
    sinkRecords.add(new SinkRecord(TOPIC, PARTITION, null, null, null, null, 1));
    sinkRecords.addAll(createRecordsWithPrimitive(4, 3,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION))));
    task.put(sinkRecords);
    task.close(mockContext.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};

    // expect sink records like the ones we put, but without the null records
    List<SinkRecord> expectedSinkRecords = createRecordsWithPrimitive(3, 0,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION)));
    expectedSinkRecords.addAll(createRecordsWithPrimitive(4, 3,
        Collections.singleton(new TopicPartition(TOPIC, PARTITION))));
    verify(expectedSinkRecords, validOffsets);
    Mockito.verify(reporter, times(2)).report(any(), any(DataException.class));
  }

  @Test
  public void testWriteRecordWithPrimitives() throws Exception {
    setUp();
    replayAll();
    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    List<SinkRecord> sinkRecords = createRecordsWithPrimitive(7, 0, Collections.singleton(new TopicPartition (TOPIC, PARTITION)));
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testRecoveryWithPartialFile() throws Exception {
    setUp();

    // Upload partial file.
    List<SinkRecord> sinkRecords = createRecords(2);
    byte[] partialData = AvroUtils.putRecords(sinkRecords, format.getAvroData());
    String fileKey = FileUtils.fileKeyToCommit(topicsDir, getDirectory(), TOPIC_PARTITION, 0, EXTENSION, ZERO_PAD_FMT);
    s3Client.putObject(PutObjectRequest.builder().bucket( S3_TEST_BUCKET_NAME).key( fileKey).build(), RequestBody.fromInputStream(new ByteArrayInputStream(partialData), partialData.length));

    // Accumulate rest of the records.
    sinkRecords.addAll(createRecords(5, 2));

    replayAll();
    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();
    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 3, 6};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testWriteRecordsSpanningMultipleParts() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "10000");
    setUp();

    List<SinkRecord> sinkRecords = createRecords(11000);
    replayAll();

    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 10000};
    verify(sinkRecords, validOffsets);
  }

  @Test
  public void testPartitionerConfig() throws Exception {
    localProps.put(PartitionerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
    localProps.put("custom.partitioner.config", "arbitrary value");
    setUp();
    replayAll();
    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
  }

  @Test
  public void testWriteRecordsWithDigestMultipleParts() throws Exception {
    localProps.put(S3SinkConnectorConfig.FLUSH_SIZE_CONFIG, "10000");
    localProps.put(S3SinkConnectorConfig.SEND_DIGEST_CONFIG, "true");
    setUp();

    List<SinkRecord> sinkRecords = createRecords(11000);
    replayAll();

    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    task.put(sinkRecords);
    task.close(context.assignment());
    task.stop();

    long[] validOffsets = {0, 10000};
    verify(sinkRecords, validOffsets);
  }

  public static class CustomPartitioner<T> extends DefaultPartitioner<T> {
    @Override
    public void configure(Map<String, Object> map) {
      assertTrue("Custom parameters were not passed down to the partitioner implementation",
          map.containsKey("custom.partitioner.config"));
    }
  }

  @Test
  public void testAclCannedConfig() throws Exception {
    localProps.put(S3SinkConnectorConfig.ACL_CANNED_CONFIG, ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL.toString());
    setUp();
    replayAll();
    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
  }

  @Test
  public void testPutRecordWithTransformedTopicNameThrowsException() throws Exception {
    setUp();
    replayAll();
    task = new S3SinkTask();
    task.initialize(context);
    task.start(properties);
    verifyAll();

    // Create a record with a different topic name (simulating what happens when a transform
    // like RegexRouter modifies the topic name)
    SinkRecord recordWithTransformedTopic = new SinkRecord(
        "KAFKA_MAIN." + TOPIC,  // Transformed topic name
        PARTITION,
        Schema.STRING_SCHEMA,
        "key",
        Schema.STRING_SCHEMA,
        "value",
        0
    );

    // This should throw ConnectException due to missing TopicPartitionWriter
    try {
      task.put(Collections.singletonList(recordWithTransformedTopic));
      fail("Expected ConnectException to be thrown");
    } catch (ConnectException e) {
      // Verify the complete error message
      String expectedMessage = "No writer found for topic partition KAFKA_MAIN.test-topic-12. "
          + "The record's topic-partition does not match any assigned partitions. "
          + "Assigned partitions: [test-topic-12, test-topic-13]";
      assertEquals("Error message should match expected format", expectedMessage, e.getMessage());
    }
  }

  private static final String RENAMED_TOPIC = "renamed-topic";
  private static final String UNASSIGNED_TOPIC = "unassigned-topic";

  // Mimics a topic-renaming SMT: record carries the new topic, original consumer record keeps the source.
  // Public constructor the runtime itself uses to carry the pre-transform topic, partition, offset.
  private SinkRecord sinkRecordWithOriginal(
      String topic,
      SinkRecord r,
      String originalTopic,
      int originalPartition,
      long originalOffset
  ) {
    return new SinkRecord(topic, r.kafkaPartition(), r.keySchema(), r.key(), r.valueSchema(),
        r.value(), r.kafkaOffset(), r.timestamp(), r.timestampType(), r.headers(),
        originalTopic, originalPartition, originalOffset);
  }

  private List<SinkRecord> createRenamedRecords(int size, String newTopic) {
    List<SinkRecord> renamed = new ArrayList<>();
    for (SinkRecord r : createRecordsWithPrimitive(
        size, 0, Collections.singleton(new TopicPartition(TOPIC, PARTITION)))) {
      renamed.add(sinkRecordWithOriginal(newTopic, r, TOPIC, PARTITION, r.kafkaOffset()));
    }
    return renamed;
  }

  private SinkTaskContext startTaskWithMockContext(ErrantRecordReporter reporter) {
    SinkTaskContext mockContext = mock(SinkTaskContext.class);
    when(mockContext.assignment()).thenReturn(Collections.singleton(TOPIC_PARTITION));
    if (reporter != null) {
      when(mockContext.errantRecordReporter()).thenReturn(reporter);
    }
    task = new S3SinkTask();
    task.initialize(mockContext);
    task.start(properties);
    return mockContext;
  }

  @Test
  public void testPutRenamedTopicRegistersSourcePartition() throws Exception {
    setUp();
    replayAll();
    startTaskWithMockContext(null);

    task.put(createRenamedRecords(3, RENAMED_TOPIC));
    Map<TopicPartition, OffsetAndMetadata> offsets = task.preCommit(Collections.emptyMap());
    task.stop();

    TopicPartition source = new TopicPartition(TOPIC, PARTITION);
    assertEquals(Collections.singleton(source), offsets.keySet());
    assertEquals(3L, offsets.get(source).offset());
  }

  @Test
  public void testPutRenamedTopicPausesAndResumesSourcePartition() throws Exception {
    setUp();
    replayAll();
    SinkTaskContext mockContext = startTaskWithMockContext(null);

    task.put(createRenamedRecords(3, RENAMED_TOPIC));
    task.stop();

    TopicPartition source = new TopicPartition(TOPIC, PARTITION);
    TopicPartition renamed = new TopicPartition(RENAMED_TOPIC, PARTITION);
    Mockito.verify(mockContext, atLeastOnce()).pause(source);
    Mockito.verify(mockContext, atLeastOnce()).resume(source);
    Mockito.verify(mockContext, never()).pause(renamed);
    Mockito.verify(mockContext, never()).resume(renamed);
  }

  @Test
  public void testPutUnassignedPlainRecordThrows() throws Exception {
    setUp();
    replayAll();
    startTaskWithMockContext(null);

    SinkRecord plain = new SinkRecord(UNASSIGNED_TOPIC, PARTITION, null, null,
        Schema.INT32_SCHEMA, 1, 0);
    try {
      task.put(Collections.singletonList(plain));
      fail("Expected ConnectException for record without an assigned writer");
    } catch (ConnectException expected) {
      // ClassCastException would escape and fail the test.
    } finally {
      task.stop();
    }
  }

  @Test
  public void testPutUnassignedRecordWithSameOriginalTopicPartitionThrows() throws Exception {
    setUp();
    replayAll();
    startTaskWithMockContext(null);

    SinkRecord unassigned = new SinkRecord(UNASSIGNED_TOPIC, PARTITION, null, null,
        Schema.INT32_SCHEMA, 1, 0, null, TimestampType.NO_TIMESTAMP_TYPE, null,
        UNASSIGNED_TOPIC, PARTITION, 0L);
    try {
      task.put(Collections.singletonList(unassigned));
      fail("Expected ConnectException when original and transformed partitions match");
    } catch (ConnectException expected) {
      // Not a rename, so upstream's guard applies.
    } finally {
      task.stop();
    }
  }

  @Test
  public void testPutNullValueOnRenamedTopicReportsDlqAndRegistersNothing() throws Exception {
    setUp();
    replayAll();
    ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
    properties.put(S3SinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG,
        S3SinkConnectorConfig.IgnoreOrFailBehavior.IGNORE.toString());
    startTaskWithMockContext(reporter);

    SinkRecord nullValue = new SinkRecord(RENAMED_TOPIC, PARTITION, null, null,
        Schema.OPTIONAL_STRING_SCHEMA, null, 0, null, TimestampType.NO_TIMESTAMP_TYPE, null,
        TOPIC, PARTITION, 0L);
    task.put(Collections.singletonList(nullValue));
    Map<TopicPartition, OffsetAndMetadata> offsets = task.preCommit(Collections.emptyMap());
    task.stop();

    Mockito.verify(reporter, times(1)).report(any(), any(DataException.class));
    assertTrue(offsets.isEmpty());
  }

  @Test
  public void testRetriableFailureOnRenamedPartitionRewindsSourcePartition() throws Exception {
    setUp();
    replayAll();
    SinkTaskContext mockContext = startTaskWithMockContext(null);
    task.put(createRenamedRecords(2, RENAMED_TOPIC));

    task.handleRetriableWriteFailure(new TopicPartition(RENAMED_TOPIC, PARTITION));
    task.stop();

    Mockito.verify(mockContext).offset(eq(new TopicPartition(TOPIC, PARTITION)), anyLong());
    Mockito.verify(mockContext, never())
        .offset(eq(new TopicPartition(RENAMED_TOPIC, PARTITION)), anyLong());
  }

  @Test
  public void testRetriableFailureKeepsSourcePartitionOnReplacementWriter() throws Exception {
    setUp();
    replayAll();
    // Replacement writer skips writes until the backoff passes.
    properties.put(S3SinkConnectorConfig.RETRY_BACKOFF_CONFIG, "1");
    startTaskWithMockContext(null);
    task.put(createRenamedRecords(2, RENAMED_TOPIC));

    task.handleRetriableWriteFailure(new TopicPartition(RENAMED_TOPIC, PARTITION));
    Thread.sleep(50);
    task.put(createRenamedRecords(3, RENAMED_TOPIC));
    Map<TopicPartition, OffsetAndMetadata> offsets = task.preCommit(Collections.emptyMap());
    task.stop();

    assertEquals(
        Collections.singleton(new TopicPartition(TOPIC, PARTITION)),
        offsets.keySet()
    );
  }

  // Tests compile against an older Connect runtime than production, so an internal-class call
  // that was removed later (InternalSinkRecord.originalRecord) is invisible to every other test.
  @Test
  public void testTaskClassesDoNotReferenceInternalConnectRuntime() throws Exception {
    for (Class<?> c : new Class<?>[] {S3SinkTask.class, TopicPartitionWriter.class}) {
      String resource = c.getName().replace('.', '/') + ".class";
      java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
      try (java.io.InputStream in = c.getClassLoader().getResourceAsStream(resource)) {
        byte[] buf = new byte[4096];
        for (int n = in.read(buf); n >= 0; n = in.read(buf)) {
          out.write(buf, 0, n);
        }
      }
      String content = new String(out.toByteArray(), java.nio.charset.StandardCharsets.ISO_8859_1);
      assertTrue(c.getSimpleName() + " references org.apache.kafka.connect.runtime",
          !content.contains("org/apache/kafka/connect/runtime/"));
    }
  }
}
