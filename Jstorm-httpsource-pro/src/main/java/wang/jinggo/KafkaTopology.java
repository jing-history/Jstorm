package wang.jinggo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangyj
 * @description
 * @create 2018-08-31 11:08
 **/
public class KafkaTopology {

    private static final String KAFKA_SPOUT_ID = "kafka-stream";
    private static final String PARSE_BOLT_ID = "parse-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        String zkHosts = "192.168.5.223:2181";
        String topicName="base64";
        String zkRoot = "";
        String zkSpoutId ="mytopic"; // 读取的offset会被存储在/zkRoot/id下面，所以id类似consumer group

        BrokerHosts hosts = new ZkHosts(zkHosts);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, zkRoot, zkSpoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        List<String> servers = new ArrayList<String>();
        servers.add("192.168.5.223");
        spoutConfig.zkServers = servers;//记录Spout读取进度所用的zookeeper的host（必须设置，否则无法记录进度）
        spoutConfig.zkPort = 2181;//记录进度用的zookeeper的端口（必须设置，否则无法记录进度）
        KafkaSpout spout = new KafkaSpout(spoutConfig);
        ParseBolt parseBolt = new ParseBolt();
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(KAFKA_SPOUT_ID, spout);
        builder.setBolt(PARSE_BOLT_ID, parseBolt).shuffleGrouping(KAFKA_SPOUT_ID);

        Config config = new Config();
        config.setNumWorkers(1);
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        } else {
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
    }
}
