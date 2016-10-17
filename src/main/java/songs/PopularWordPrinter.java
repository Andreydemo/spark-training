package songs;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by andrii_korkoshko on 16.10.16.
 */
@Service
public class PopularWordPrinter {

    @Autowired
    private JavaSparkContext sc;

    @Autowired
    private TopWordsService  topWordsService;


    public void printTopX(String pathToDIr , int x){
        topWordsService.topX(sc.textFile(pathToDIr),x).forEach(System.out::println);
    }
}
