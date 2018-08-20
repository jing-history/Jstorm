package topology;

import bolt.SplitSentence;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import scala.actors.threadpool.Arrays;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangyj
 * @description
 * @create 2018-08-20 10:34
 **/
public class WordCountTopology {
    public static void main(String[] args) {
        //zookeeper的服务器地址
        String zks = "192.168.1.222:2181";
        //消息的topic
        String topic = "storm-sentence";
        //strom在zookeeper上的根
        String zkRoot = "/storm";
        String id = "storm";
    //    SpoutConfig kafkaConf = new SpoutConfig(StaticHosts.fromHostString(hosts, 10), "storm-sentence", "", "storm");

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList(new String[]{"192.168.1.222"});
        spoutConfig.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("1", new KafkaSpout(spoutConfig), 10);// id, spout,
        // parallelism_hint
        builder.setBolt("2", new SplitSentence(), 10).shuffleGrouping("1");
    }
}
