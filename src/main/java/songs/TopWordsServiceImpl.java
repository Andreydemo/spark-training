package songs;

import org.apache.ivy.util.PropertiesFile;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by andrii_korkoshko on 16.10.16.
 */
@Component
public class TopWordsServiceImpl implements TopWordsService {

    @Autowired
    UserConfig userConfig;

    @AutowiredBroadcast
    private Broadcast<UserConfig> userConfigBroadcast;


    @Override
    public List<String> topX(JavaRDD<String> lines, int x) {
        List<String> list = userConfig.ignoreList;
        return lines.map(String::toLowerCase).flatMap(WordsUtil::getWords)
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .map(Tuple2::_2).filter(it -> !userConfigBroadcast.value().ignoreList.contains(it)).take(x);
    }


}
