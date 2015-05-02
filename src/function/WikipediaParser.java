package storm.starter.web.crawler.src.function;

import backtype.storm.tuple.Values;
import com.google.gson.Gson;
import storm.starter.web.crawler.src.dataclasses.PageData;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.Arrays;


/**
 * Created by Abidaan Nagawkar on 5/1/15.
 */
public class WikipediaParser extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String jsonString = (String) tuple.getValue(0);

        Gson gson = new Gson();
        PageData data = gson.fromJson(jsonString, PageData.class);

        StringBuilder builder = new StringBuilder();
        for(String s: data.getCategories()){
            builder.append(s);
            builder.append("||");
        }

        collector.emit(new Values(data.getBaseurl(), builder.toString().trim(), Arrays.asList(data.getUrls())));
    }
}
