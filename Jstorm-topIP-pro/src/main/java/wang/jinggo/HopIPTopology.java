package wang.jinggo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import scala.actors.threadpool.Arrays;

/**
 * @author wangyj
 * @description
 * @create 2018-08-20 10:06
 **/
public class HopIPTopology {
    public static void main(String[] args) {
        //zookeeper的服务器地址
        String zks = "192.168.1.222:2181";
        //消息的topic
        String topic = "mytopic";
        //strom在zookeeper上的根
        String zkRoot = "/storm";
        String id = "mytopic";
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList(new String[]{"192.168.1.222"});
        spoutConfig.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        //指定的任务的spout组件，从Kafka中获取数据
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConfig));

        //指定任务的第一个bolt组件，解析log信息，进行分词
        builder.setBolt("split_blot", new HotIPSplitBolt()).shuffleGrouping("kafka-reader");

        //指定任务的第二个bolt组件，计算hot ip
        builder.setBolt("hotip_blot",new HotIPTotalBolt()).fieldsGrouping("split_blot",new Fields("ip"));

        Config  conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mydemo", conf, builder.createTopology());

        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        cluster.shutdown();
    }
}
