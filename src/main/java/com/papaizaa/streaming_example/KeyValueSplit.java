package com.papaizaa.streaming_example;

import com.papaizaa.streaming_example.generated_pb.Events;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyValueSplit {

    public static class Parse extends DoFn<Events.Event, KV<String, Events.Event>> {
        private static final Logger LOG = LoggerFactory.getLogger(KeyValueSplit.class);


        @ProcessElement
        public void processElement(ProcessContext c){
            try {
                Events.Event element = c.element();
                // Check if the values returned from BigQuery are null, as all columns in BigQuery are nullable

                if (element.getUserId().equals("")){
                    LOG.error("PROTO input: {} has no USERID: ", c.element());
                }

                if (element.getEventTime().equals("")){
                    LOG.error("PROTO input: {} has no EVENT TIME: ", c.element());
                }

                if (element.getPrice() <= 0.0){
                    LOG.error("PROTO input: {} has no PRICE: ", c.element());
                }

                String key = element.getUserId();

                c.output(KV.of(key, element));
            } catch (Exception e){
                LOG.error("Error reading PROTO input: {} with error message: ", c.element(), e);
            }
        }
    }

}
