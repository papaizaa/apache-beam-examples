package com.papaizaa.batch_example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Lists;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.LocalDateTime;

import java.util.ArrayList;
import java.util.List;

public class JoinEvents {

    public static class Join extends DoFn<KV<String, CoGbkResult>, TableRow> {
        private final TupleTag<Iterable<Double>> booksTag;
        private final TupleTag<Iterable<Double>> groceriesTag;

        public Join(TupleTag<Iterable<Double>> booksTag,
                    TupleTag<Iterable<Double>> groceriesTag) {
            this.booksTag = booksTag;
            this.groceriesTag = groceriesTag;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<String, CoGbkResult> e = c.element();

            String userId = e.getKey();

            Iterable<Double> books = e.getValue().getOnly(booksTag, null);
            Iterable<Double> groceries = e.getValue().getOnly(groceriesTag, null);

            List<Double> bookList = books == null ? new ArrayList<>() : Lists.newArrayList(books);
            List<Double> groceryList = groceries == null ? new ArrayList<>() : Lists.newArrayList(groceries);

            double bookTotal = bookList.stream().mapToDouble(Double::doubleValue).sum();
            double groceryTotal = groceryList.stream().mapToDouble(Double::doubleValue).sum();

            LocalDateTime endDate = LocalDateTime.now()
                    .withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
            LocalDateTime startDate = endDate.minusDays(7);

            c.output(sumToTableRow(userId, bookTotal + groceryTotal, startDate, endDate));
        }

        public static TableRow sumToTableRow(String userId, double total, LocalDateTime startDate, LocalDateTime endDate){
            TableRow row = new TableRow();
            row.set("UserID", userId);
            row.set("TotalSalesInWeek", total);
            row.set("StartOfWeek", startDate.toString());
            row.set("EndOfWeek", endDate.toString());

            return row;
        }
    }

}
