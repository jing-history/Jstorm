package wang.jinggo;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author wangyj
 * @description
 * @create 2018-09-02 15:46
 **/
public class Main {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new ReaderSpout(), 4);
        builder.setBolt("area-bolt", new GetAreaBolt()).shuffleGrouping("spout");
        builder.setBolt("longitude-bolt", new GetLongitudeBolt()).shuffleGrouping("area-bolt");

        Config config = new Config();
        config.setNumWorkers(4);
        config.setMaxSpoutPending(1000);
        StormSubmitter.submitTopology("area-topology", config, builder.createTopology());
    }
}
