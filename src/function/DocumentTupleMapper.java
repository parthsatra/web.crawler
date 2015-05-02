package storm.starter.web.crawler.src.function;

import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.mapper.TridentTupleMapper;
import storm.trident.tuple.TridentTuple;

/**
 * Created by Parth Satra on 5/2/15.
 */
public class DocumentTupleMapper implements TridentTupleMapper<Document<String>> {

    @Override
    public Document<String> map(TridentTuple tridentTuple) {
        String url = tridentTuple.getString(0);
        String categories = tridentTuple.getString(1);

        Document<String> document = new Document<String>(Constants.INDEX, Constants.TYPE, categories, url);
        return document;
    }
}
