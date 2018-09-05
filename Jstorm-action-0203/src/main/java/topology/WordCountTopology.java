package topology;

import bolt.SplitSentence;
import bolt.WordCountBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author wangyj
 * @description
 * @create 2018-08-20 10:34
 **/
public class WordCountTopology {
    public static void main(String[] args) {
        //zookeeper的服务器地址
        String zks = "192.168.5.223:2181";
        //消息的topic
        String topic = "storm-sentence";
        //strom在zookeeper上的根
        String zkRoot = "";
        String id = "storm-sentence";
    //    SpoutConfig kafkaConf = new SpoutConfig(StaticHosts.fromHostString(hosts, 10), "storm-sentence", "", "storm");

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList(new String[]{"192.168.5.223"});
        spoutConfig.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("1", new KafkaSpout(spoutConfig), 10);// id, spout,
        // parallelism_hint
        builder.setBolt("2", new SplitSentence(), 10).shuffleGrouping("1");
        builder.setBolt("3", new WordCountBolt(), 20).fieldsGrouping("2", new Fields("word"));

        Config config = new Config();
        //本地测试storm
        LocalCluster cluster = new LocalCluster();
        config.setDebug(true);
        cluster.submitTopology("sentence_word_count", config, builder.createTopology());
        /*try {
            try {
                StormSubmitter.submitTopology("sentence_word_count", config,
                        builder.createTopology());
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }

        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }*/
    }
}
