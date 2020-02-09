package com.papaizaa.streaming_example;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.api.services.bigquery.model.TableRow;
import com.papaizaa.batch_example.ParseTableData;
import com.papaizaa.streaming_example.generated_pb.Events;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class KeyValueSplitTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();


    @Test
    public void testParseProtoEventSuccess(){

        Events.Event event =
                Events.Event.newBuilder()
                        .setEventId("a")
                        .setEventTime("2019-01-01T12:22:22Z")
                        .setUserId("a1")
                        .setPrice(12.4f)
                        .setEventType(Events.Event.EventType.GROCERY_SALE)
                        .build();

        PCollection<KV<String, Events.Event>> out =
                testPipeline
                        .apply("Create Event", Create.of(event)).setCoder(ProtoCoder.of(Events.Event.class))
                        .apply("KV Split", ParDo.of(new KeyValueSplit.Parse()));


        PAssert.that(out).containsInAnyOrder(KV.of("a1", event));

        testPipeline.run().waitUntilFinish();
    }


    @Test
    public void testFailedTableRowDataNoUserID(){

        Events.Event event =
                Events.Event.newBuilder()
                        .setEventId("a")
                        .setEventTime("2019-01-01T12:22:22Z")
                        .setUserId("")
                        .setPrice(12.4f)
                        .setEventType(Events.Event.EventType.GROCERY_SALE)
                        .build();

        PCollection<KV<String, Events.Event>> out =
                testPipeline
                        .apply("Create Event", Create.of(event)).setCoder(ProtoCoder.of(Events.Event.class))
                        .apply("KV Split", ParDo.of(new KeyValueSplit.Parse()));

        PAssert.that(out).empty();

        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testFailedTableRowDataNoEventTime(){

        Events.Event event =
                Events.Event.newBuilder()
                        .setEventId("a")
                        .setEventTime("")
                        .setUserId("a1")
                        .setPrice(12.4f)
                        .setEventType(Events.Event.EventType.GROCERY_SALE)
                        .build();

        PCollection<KV<String, Events.Event>> out =
                testPipeline
                        .apply("Create Event", Create.of(event)).setCoder(ProtoCoder.of(Events.Event.class))
                        .apply("KV Split", ParDo.of(new KeyValueSplit.Parse()));

        PAssert.that(out).empty();

        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void testFailedTableRowDataNoPrice(){

        Events.Event event =
                Events.Event.newBuilder()
                        .setEventId("a")
                        .setEventTime("2019-01-01T12:22:22Z")
                        .setUserId("a1")
                        .setPrice(0.0f)
                        .setEventType(Events.Event.EventType.GROCERY_SALE)
                        .build();

        PCollection<KV<String, Events.Event>> out =
                testPipeline
                        .apply("Create Event", Create.of(event)).setCoder(ProtoCoder.of(Events.Event.class))
                        .apply("KV Split", ParDo.of(new KeyValueSplit.Parse()));

        PAssert.that(out).empty();

        testPipeline.run().waitUntilFinish();
    }

}
