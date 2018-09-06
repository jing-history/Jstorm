package wang.jinggo.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

/**
 * @author wangyj
 * @description
 * @create 2018-09-06 11:34
 **/
public class FlowMain {

    public static void main(String[] args) {
        /*TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new ParallelFileSpout());
        builder.setBolt("word-normalizer", new DetectionBolt(), 1).fieldsGrouping("word-reader", new Fields("word1"));
        builder.setBolt("word-counter", new CountBolt()).fieldsGrouping("word-normalizer", new Fields("word"));
        Config conf = new Config();
        try {
            StormSubmitter.submitTopology("wordCounterTopology", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }*/

        //zookeeper的服务器地址
        String zks = "192.168.5.223:2181";
        //消息的topic
        String topic = "storm-flow";
        //strom在zookeeper上的根
        String zkRoot = "";
        String id = "storm-flow";
        //    SpoutConfig kafkaConf = new SpoutConfig(StaticHosts.fromHostString(hosts, 10), "storm-sentence", "", "storm");

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList(new String[]{"192.168.5.223"});
        spoutConfig.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new ParallelFileSpout(), 4);
        builder.setBolt("word-normalizer", new DetectionBolt()).fieldsGrouping("word-reader", new Fields("word1"));
        builder.setBolt("word-counter", new CountBolt()).fieldsGrouping("word-normalizer", new Fields("word"));

        Config config = new Config();
        //本地测试storm
        LocalCluster cluster = new LocalCluster();
        config.setDebug(true);
        cluster.submitTopology("sentence_wall", config, builder.createTopology());
    }
}
