package songs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by andrii_korkoshko on 17.10.16.
 */
@Component
public class UserConfig implements Serializable {

    public List<String> ignoreList;

    @Value("${garbage}")
    private void setIgnoreList(String[] ignoreList) {
        this.ignoreList = Arrays.asList(ignoreList);
    }
}
