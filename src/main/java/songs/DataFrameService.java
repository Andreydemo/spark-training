package songs;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by andrii_korkoshko on 17.10.16.
 */
@Component
public class DataFrameService {


    public static final String KEYWORDS_COLL_NAME = "keywords";
    public static final String NAME_COL_NAME = "name";
    public static final String SALARY_COLL_NAME = "salary";

    public void print(DataFrame dataFrame) {
        dataFrame.show();
    }

    public void printSchema(DataFrame dataFrame) {
        dataFrame.printSchema();
    }

    public void printSchemaColumns(DataFrame dataFrame) {
        Arrays.asList(dataFrame.schema().fields()).forEach(it -> System.out.println(it.dataType()));
    }

    public DataFrame buildSalary(DataFrame dataFrame) {
        return dataFrame.withColumn(SALARY_COLL_NAME, functions.column("age").multiply(10).multiply(functions.size(functions.column(KEYWORDS_COLL_NAME))));
    }

    public String printChippestTopDev(DataFrame dataFrame) {
        DataFrame sllaries = buildSalary(dataFrame);
        String top = getKeywordsTop(dataFrame);
        DataFrame chippestRow = sllaries.where(functions.col(SALARY_COLL_NAME).leq(1200).and(functions.array_contains(functions.col(KEYWORDS_COLL_NAME), top)));
        return chippestRow.select(NAME_COL_NAME).collectAsList().stream().map(it -> it.getString(0)).collect(Collectors.toList()).get(0);

    }


    private String getKeywordsTop(DataFrame dataFrame) {
        DataFrame keywords = dataFrame.select(functions.explode(functions.column(KEYWORDS_COLL_NAME)));
        String rows = keywords.groupBy(functions.column("col")).count().orderBy(functions.desc("count")).
                select(functions.col("col")).first().getString(0);
        System.out.println(rows);
        return rows;
    }
}
