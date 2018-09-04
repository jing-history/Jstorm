package wang.jinggo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

/**
 * @author wangyj
 * @description
 * @create 2018-09-04 10:58
 **/
public class WallLocalMain {
    public static void main(String[] args) {
        //zookeeper的服务器地址
        String zks = "192.168.5.223:2181";
        //消息的topic
        String topic = "storm-wall";
        //strom在zookeeper上的根
        String zkRoot = "";
        String id = "storm-wall";
        //    SpoutConfig kafkaConf = new SpoutConfig(StaticHosts.fromHostString(hosts, 10), "storm-sentence", "", "storm");

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList(new String[]{"192.168.5.223"});
        spoutConfig.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new ReaderSpout(), 4);
        builder.setBolt("area-bolt", new GetAreaBolt()).shuffleGrouping("spout");
        builder.setBolt("longitude-bolt", new GetLongitudeBolt()).shuffleGrouping("area-bolt");

        Config config = new Config();
        //本地测试storm
        LocalCluster cluster = new LocalCluster();
        config.setDebug(true);
        cluster.submitTopology("sentence_wall", config, builder.createTopology());
    }
}
