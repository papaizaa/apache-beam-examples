package come.papaizaa.batch_example;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.api.services.bigquery.model.TableRow;
import com.papaizaa.batch_example.JoinEvents;
import com.papaizaa.batch_example.ParseTableData;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.LocalDateTime;
import org.junit.Rule;
import org.junit.Test;

public class JoinEventsTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();


    @Test
    public void testParseTableRowDataSuccess(){

        ImmutableList<TableRow> inputBooks = ImmutableList.of(
                new TableRow()
                        .set("UserID", "1")
                        .set("Price", 20.1),
                new TableRow()
                        .set("UserID", "1")
                        .set("Price", 20.0));

        ImmutableList<TableRow> inputGroceries = ImmutableList.of(
                new TableRow()
                        .set("UserID", "1")
                        .set("Price", 85.0));

        LocalDateTime endDate = LocalDateTime.now()
                .withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfSecond(0);
        LocalDateTime startDate = endDate.minusDays(7);

        final PCollectionView<LocalDateTime> dateStartView = testPipeline
                .apply("DateStart to Pcollection", Create.of(startDate))
                .apply("DateStart as View", View.asSingleton());
        final PCollectionView<LocalDateTime> dateEndView = testPipeline
                .apply("DateEnd to Pcollection", Create.of(endDate))
                .apply("DateEnd as View", View.asSingleton());

        PCollection<KV<String, Iterable<Double>>> books = testPipeline
                .apply("Create books input", Create.of(inputBooks))
                .apply("Parse Books",
                        ParDo.of(new ParseTableData.Parse()))
                .apply("Groupby books", GroupByKey.create());

        PCollection<KV<String, Iterable<Double>>> groceries = testPipeline
                .apply("Create grocery input", Create.of(inputGroceries))
                .apply("Parse Groceries",
                        ParDo.of(new ParseTableData.Parse()))
                .apply("Groupby groceries", GroupByKey.create());

        TupleTag<Iterable<Double>> booksTag = new TupleTag<>();
        TupleTag<Iterable<Double>> groceryTag = new TupleTag<>();

        PCollection<TableRow> output =
                KeyedPCollectionTuple.of(booksTag, books)
                .and(groceryTag, groceries)
                .apply("CoGroupByKey", CoGroupByKey.create())
                .apply("Join Data", ParDo.of(new JoinEvents.Join(booksTag, groceryTag, dateStartView, dateEndView))
                        .withSideInputs(dateStartView, dateEndView));


        TableRow rowUser1 = new TableRow();
        rowUser1.set("UserID", "1");
        rowUser1.set("TotalSalesInWeek", 125.1);
        rowUser1.set("StartOfWeek", startDate.toString());
        rowUser1.set("EndOfWeek", endDate.toString());

        PAssert.that(output).containsInAnyOrder(rowUser1);

        testPipeline.run();
    }

}
