package wang.jinggo.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author wangyj
 * @description
 * @create 2018-09-06 11:34
 **/
public class Main {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
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
        }
    }
}
