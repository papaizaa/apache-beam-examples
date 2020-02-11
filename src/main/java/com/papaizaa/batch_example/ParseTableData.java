package com.papaizaa.batch_example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ParseTableData {

    public static class Parse extends DoFn<TableRow, KV<String, Double>> {

        @ProcessElement
        public void processElement(ProcessContext c) {

            TableRow element = c.element();
            // Check if the values returned from BigQuery are null, as all columns in BigQuery are nullable
            String key = element.get("UserID") == null ? null : (String) element.get("UserID");
            Double price = element.get("Price") == null ? null : (Double) element.get("Price");

            if (key == null || price == null) {
                return;
            }

            c.output(KV.of(key, price));
        }
    }

}
