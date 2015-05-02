package storm.starter.web.crawler.src;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.state.ESIndexState;
import com.github.fhuss.storm.elasticsearch.state.ESIndexUpdater;
import com.github.fhuss.storm.elasticsearch.state.QuerySearchIndexQuery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.starter.web.crawler.src.function.DocumentTupleMapper;
import storm.starter.web.crawler.src.function.GenerateQuery;
import storm.starter.web.crawler.src.function.WikipediaParser;
import storm.starter.web.crawler.src.state.*;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import storm.trident.testing.Split;

/**
 * Created by Parth Satra on 5/1/15.
 */
public class WebCrawlerTopology {

    public static StormTopology buildTopology(String args[], LocalDRPC drpc) {
        TridentTopology topology = new TridentTopology();

        BrokerHosts zk = new ZkHosts("152.46.20.89:2181");

        // Configuration for connecting to Kafka for bloom filter
        TridentKafkaConfig spoutConfig1 = new TridentKafkaConfig(zk, "crawler_feed1");
        spoutConfig1.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spoutWiki1 = new OpaqueTridentKafkaSpout(spoutConfig1);

        // Configuration for connecting to Kafka for countmin sketch
        TridentKafkaConfig spoutConfig2 = new TridentKafkaConfig(zk, "crawler_feed2");
        spoutConfig2.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spoutWiki2 = new OpaqueTridentKafkaSpout(spoutConfig2);

        // Configuration for connecting to Kafka for countmin sketch
        TridentKafkaConfig spoutConfig3 = new TridentKafkaConfig(zk, "crawler_feed3");
        spoutConfig3.scheme = new SchemeAsMultiScheme(new StringScheme());
        OpaqueTridentKafkaSpout spoutWiki3 = new OpaqueTridentKafkaSpout(spoutConfig3);

        // Creating a bloom filter with size 1000
        int bloomFilterSize = 1000;
        BloomFilterStateFactory bloomFilterStateFactory = new BloomFilterStateFactory(bloomFilterSize);

        // Creating a count min sketch factory
        int width = 1000;
        int depth = 5;
        int seed = 10;
        CountMinSketchStateFactory countMinSketchStateFactory = new CountMinSketchStateFactory(depth, width, seed);

        // Elastic Search factory
        Settings esSettings = ImmutableSettings.settingsBuilder()
                .put("storm.elasticsearch.cluster.name", "elasticsearch")
                .put("storm.elasticsearch.hosts", "152.46.18.173:9300")
                .build();
        StateFactory esStateFactory = new ESIndexState.Factory<String>(new ClientFactory.NodeClient(esSettings.getAsMap()), String.class);
        TridentState staticEsState = topology.newStaticState(new ESIndexState.Factory<String>(new ClientFactory.NodeClient(esSettings.getAsMap()), String.class));

        // Topology that checks if the URL is already present.
        TridentState bloomFilterDBMS = topology.newStream("bloomfilter", spoutWiki1)
                .each(spoutWiki1.getOutputFields(), new WikipediaParser(), new Fields("Url", "categories", "outgoingUrls"))

                .partitionPersist(bloomFilterStateFactory, new Fields("Url"), new BloomFilterStateUpdater());

        TridentState countminDBMS = topology.newStream("countmin", spoutWiki2)
                .each(spoutWiki2.getOutputFields(), new WikipediaParser(), new Fields("Url", "categories", "outgoingUrls"))
                .partitionPersist(countMinSketchStateFactory, new Fields("outgoingUrls"), new CountMinSketchUpdater());

        TridentState esStateDBMS = topology.newStream("esearch", spoutWiki3)
                .each(spoutWiki3.getOutputFields(), new WikipediaParser(), new Fields("Url", "categories", "outgoingUrls"))
                .partitionPersist(esStateFactory, new Fields("Url", "categories"), new ESIndexUpdater(new DocumentTupleMapper()));

        // DRPC query that returns if the URL is already visited.
        topology.newDRPCStream("check_url", drpc)
                .each(new Fields("args"), new Split(), new Fields("query"))
                .stateQuery(bloomFilterDBMS, new Fields("query"), new BloomFilterStateQuery(), new Fields("result"))
                .project(new Fields("result"));

        topology.newDRPCStream("search", drpc)
                .each(new Fields("args"), new GenerateQuery(), new Fields("query", "index", "type"))
                .stateQuery(staticEsState, new Fields("query", "index", "type"), new QuerySearchIndexQuery(), new Fields("Urls"))
                //.stateQuery(countminDBMS, new Fields("Urls"), new CountMinQuery(), new Fields("result"))
                .project(new Fields("result"));

        return topology.build();
    }


    public static void main(String args[]) throws Exception {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(10);

        // Configuration for running storm locally.
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();

        //Running storm distributed
        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar("Web_Crawler", conf, buildTopology(args, null));

        // Local cluster
//        cluster.submitTopology("Web_Crawler", conf, buildTopology(args, drpc));
//
//        while(true) {
//            System.out.println("DRPC RESULT: " + drpc.execute("check_url", "California_Chrome"));
//            Thread.sleep(3000);
//        }

    }
}
