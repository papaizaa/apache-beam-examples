package com.papaizaa.streaming_example;

import com.papaizaa.streaming_example.generated_pb.Events;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.sdk.transforms.windowing.AfterProcessingTime.pastFirstElementInPane;

public class CombineEventsTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    private Instant baseTime = new Instant(0);
    private static final String USER_ID = "2";
    private static final int SESSION_GAP_DURATION = 10;
    private static final int ALLOWED_LATENESS_SEC = 0;
    private List<KV<String, Events.Event>> events;
    private Window<KV<String, Events.Event>> sessionWindow;

    @Before
    public void setUp() {
        events = new ArrayList<>();

        Trigger trigger = Repeatedly.forever(pastFirstElementInPane()
                .orFinally(AfterWatermark.pastEndOfWindow()));

        sessionWindow =
                Window.<KV<String, Events.Event>>into(Sessions.withGapDuration(
                        Duration.standardMinutes(SESSION_GAP_DURATION)))
                        .triggering(trigger)
                        .withAllowedLateness(Duration.standardSeconds(ALLOWED_LATENESS_SEC))
                        .accumulatingFiredPanes();

    }

    @Test
    public void testPipelineWithTrueEventsAndAllWithinWindow() throws IOException {

        events.add(createDummyEvent(4.0f, Duration.standardMinutes(2), "b1"));
        events.add(createDummyEvent(30.2f, Duration.standardMinutes(3), "b2"));
        events.add(createDummyEvent(0.6f, Duration.standardMinutes(1), "a1"));
        events.add(createDummyEvent(11.4f, Duration.standardMinutes(4), "b3"));

        PCollection<KV<String, Events.Event>> combinedEvents = testPipeline.apply("Create Events", TestStream.create(KvCoder.of(StringUtf8Coder.of(), ProtoCoder.of(Events.Event.class)))
                .advanceWatermarkTo(baseTime)
                .addElements(createTSEvent(events.get(2)))
                .advanceProcessingTime(Duration.standardMinutes(2))
                .addElements(createTSEvent(events.get(0)))
                .advanceProcessingTime(Duration.standardMinutes(3))
                .addElements(createTSEvent(events.get(1)))
                .advanceProcessingTime(Duration.standardMinutes(4))
                .addElements(createTSEvent(events.get(3)))
                .advanceProcessingTime(Duration.standardMinutes(5))
                .advanceProcessingTime(Duration.standardMinutes(15)) // Advance processing time to after watermark
                .advanceWatermarkToInfinity());

        PCollection<Events.Output> output = applyCombineTransform(sessionWindow, combinedEvents);

        ArrayList<Events.Event> expectedEvents = new ArrayList<Events.Event>() {{
            add(events.get(2).getValue());
        }};

        Events.Output resultFirst = Events.Output.newBuilder()
                .setWindowId("a1")
                .setUserId(USER_ID)
                .setWindowStart(baseTime.plus(Duration.standardMinutes(1)).toString())
                .setWindowEnd("1970-01-01T00:01:00.000Z")
                .setTotalPrice(0.6f)
                .setWindowOpen(true)
                .addAllEvents(expectedEvents)
                .build();

        expectedEvents.add(events.get(0).getValue());

        Events.Output resultSecond = Events.Output.newBuilder()
                .setWindowId("a1")
                .setUserId(USER_ID)
                .setWindowStart(baseTime.plus(Duration.standardMinutes(1)).toString())
                .setWindowEnd("1970-01-01T00:02:00.000Z")
                .setTotalPrice(4.6f)
                .setWindowOpen(true)
                .addAllEvents(expectedEvents)
                .build();

        expectedEvents.add(events.get(1).getValue());

        Events.Output resultThird = Events.Output.newBuilder()
                .setWindowId("a1")
                .setUserId(USER_ID)
                .setWindowStart(baseTime.plus(Duration.standardMinutes(1)).toString())
                .setWindowEnd("1970-01-01T00:03:00.000Z")
                .setTotalPrice(34.8f)
                .setWindowOpen(true)
                .addAllEvents(expectedEvents)
                .build();

        expectedEvents.add(events.get(3).getValue());

        Events.Output resultFourth = Events.Output.newBuilder()
                .setWindowId("a1")
                .setUserId(USER_ID)
                .setWindowStart(baseTime.plus(Duration.standardMinutes(1)).toString())
                .setWindowEnd("1970-01-01T00:04:00.000Z")
                .setTotalPrice(46.2f)
                .setWindowOpen(true)
                .addAllEvents(expectedEvents)
                .build();

        Events.Output resultAfterWatermark = Events.Output.newBuilder()
                .setWindowId("a1")
                .setUserId(USER_ID)
                .setWindowStart(baseTime.plus(Duration.standardMinutes(1)).toString())
                .setWindowEnd("1970-01-01T00:13:59.999Z")
                .setTotalPrice(46.2f)
                .setWindowOpen(false)
                .addAllEvents(expectedEvents)
                .build();


        IntervalWindow windowFirstEvent = new IntervalWindow(baseTime.plus(Duration.standardMinutes(1)),
                baseTime.plus(Duration.standardMinutes(11)));
        IntervalWindow windowSecondEvent = new IntervalWindow(baseTime.plus(Duration.standardMinutes(1)),
                baseTime.plus(Duration.standardMinutes(12)));
        IntervalWindow windowThirdEvent = new IntervalWindow(baseTime.plus(Duration.standardMinutes(1)),
                baseTime.plus(Duration.standardMinutes(13)));
        IntervalWindow windowFourthEvent = new IntervalWindow(baseTime.plus(Duration.standardMinutes(1)),
                baseTime.plus(Duration.standardMinutes(14)));


        PAssert.that(output).inWindow(windowFirstEvent).containsInAnyOrder(resultFirst);
        PAssert.that(output).inWindow(windowSecondEvent).containsInAnyOrder(resultSecond);
        PAssert.that(output).inWindow(windowThirdEvent).containsInAnyOrder(resultThird);
        PAssert.that(output).inEarlyPane(windowFourthEvent).containsInAnyOrder(resultFourth);


        PAssert.that(output).inFinalPane(windowFourthEvent).containsInAnyOrder(resultAfterWatermark);
        testPipeline.run().waitUntilFinish();
    }

    private KV<String, Events.Event> createDummyEvent(float price, Duration eventTimeOffset, String eventId) {
        return KV.of(USER_ID,
                Events.Event.newBuilder()
                        .setEventId(eventId)
                        .setEventTime(baseTime.plus(eventTimeOffset).toString())
                        .setUserId(USER_ID)
                        .setPrice(price)
                        .setEventType(Events.Event.EventType.GROCERY_SALE)
                        .build()
        );
    }

    private TimestampedValue<KV<String, Events.Event>> createTSEvent(KV<String, Events.Event> kv) throws IOException {
        return TimestampedValue.of(kv, Instant.parse(kv.getValue().getEventTime()));
    }

    private PCollection<Events.Output> applyCombineTransform(Window<KV<String, Events.Event>> window,
                                                             PCollection<KV<String, Events.Event>> inputStream) {
        return inputStream.apply("Apply to Session Window", window)
                .apply("Group By Key", GroupByKey.create())
                .apply("Parse And Format Pub Sub Message", ParDo.of(new CombineEvents.Combine()));
    }

}
