package com.papaizaa.streaming_example;

import avro.shaded.com.google.common.collect.Lists;
import com.papaizaa.streaming_example.generated_pb.Events;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;

public class CombineEvents {

    public static class Combine extends DoFn<KV<String, Iterable<Events.Event>>, Events.Output> {
        private static final Logger LOG = LoggerFactory.getLogger(KeyValueSplit.class);


        @ProcessElement
        public void processElement(ProcessContext c){
            try {
                KV<String, Iterable<Events.Event>> element = c.element();
                String userID = element.getKey();
                List<Events.Event> events = Lists.newArrayList(element.getValue());

                float totalSum = (float) events.stream().mapToDouble(Events.Event::getPrice).sum();

                List<Events.Event> sortedEvents = sortEvents(events);

                Events.Event oldestEvent = sortedEvents.get(0);

                // Lets set the windowID as the event ID of the first element in the window
                String windowEventID = oldestEvent.getEventId();
                String oldestEventTime = oldestEvent.getEventTime();
                String newestEventTime = sortedEvents.get(events.size() - 1).getEventTime();

                boolean windowOpen = c.pane().getTiming() != PaneInfo.Timing.ON_TIME;

                Events.Output out = Events.Output
                        .newBuilder()
                        .setWindowId(windowEventID)
                        .setUserId(userID)
                        .setWindowStart(oldestEventTime)
                        .setWindowEnd(newestEventTime)
                        .setTotalPrice(totalSum)
                        .setWindowOpen(windowOpen)
                        .addAllEvents(events)
                        .build();

                c.output(out);
            } catch (Exception e){
                LOG.error("Error reading PROTO input: {} with error message: ", c.element(), e);
            }
        }

        public List<Events.Event> sortEvents(List<Events.Event> events){
            Comparator<Events.Event> compareByEventTime = new Comparator<Events.Event>() {
                @Override
                public int compare(Events.Event e1, Events.Event e2)  {
                    DateTime dateE1 = DateTime.parse(e1.getEventTime());
                    DateTime dateE2 = DateTime.parse(e2.getEventTime());
                    return dateE1.compareTo(dateE2);
                }
            };
            events.sort(compareByEventTime);
            return events;
        }

    }

}
