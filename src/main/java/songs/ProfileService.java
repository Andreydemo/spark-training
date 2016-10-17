package songs;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by andrii_korkoshko on 17.10.16.
 */
@Component
public class ProfileService {

    @Autowired
    private DataFrameService dataFrameService;

    private SQLContext sqlContext;

    @Autowired
    private void setSqlContext(JavaSparkContext sc) {
        sqlContext = new SQLContext(sc);
    }

    public void print() {
        DataFrame dataFrame = getDataFrame();
        dataFrameService.print(dataFrame);
        dataFrameService.printSchema(dataFrame);
        dataFrameService.printSchemaColumns(dataFrame);
        dataFrameService.buildSalary(dataFrame).show();
        System.out.println(
                dataFrameService.printChippestTopDev(dataFrame));
    }

    private DataFrame getDataFrame() {
        return sqlContext.jsonFile("data/linkedIn/profiles.json");
    }

}
