package com.papaizaa.batch_example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;

import static org.slf4j.LoggerFactory.*;

public class BatchExampleMain {

    public static final Logger LOG = getLogger(BatchExampleMain.class);
    public static DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss zz");
    DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");

    public static void main(String[] args) {
        BatchPipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(BatchPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        LocalDateTime endDate = LocalDateTime.now()
                .withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
        LocalDateTime startDate = endDate.minusDays(7);


        PCollection<TableRow> bookData = pipeline.apply("Read Book Table", BigQueryIO.readTableRows()
                .fromQuery(
                        String.format("SELECT UserID, Price FROM `%1$s` WHERE date_time >= \"%2$s\" AND date_time < \"%3$s\"",
                                options.getBookSalesInputTable(),
                                startDate.toString(),
                                endDate.toString())
                )
                .usingStandardSql()).setCoder(TableRowJsonCoder.of());

        PCollection<TableRow> groceryData = pipeline.apply("Read Groceries Table", BigQueryIO.readTableRows()
                .fromQuery(
                        String.format("SELECT UserID, Price FROM `%1$s` WHERE date_time >= \"%2$s\" AND date_time < \"%3$s\"",
                                options.getGrocerySalesInputTable(),
                                startDate.toString(),
                                endDate.toString())
                )
                .usingStandardSql()).setCoder(TableRowJsonCoder.of());

        PCollection<KV<String, Double>> bookPurchases =
                bookData
                        .apply("Parse Sound Events", ParDo.of(new ParseTableData.Parse()));

        PCollection<KV<String, Double>> groceryPurchases =
                groceryData
                        .apply("Parse Grocery Purchases", ParDo.of(new ParseTableData.Parse()));

    }


    public interface BatchPipelineOptions extends PipelineOptions {

        @Description("The location of the BigQuery table for Book Sales data")
        ValueProvider<String> getBookSalesInputTable();

        void setBookSalesInputTable(ValueProvider<String> value);

        @Description("The location of the BigQuery table for Grocery Sales data")
        ValueProvider<String> getGrocerySalesInputTable();

        void setGrocerySalesInputTable(ValueProvider<String> value);


    }
}
