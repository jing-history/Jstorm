package wang.jinggo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

/**
 * @author wangyj
 * @description
 * @create 2018-09-02 15:46
 **/
public class ReaderSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        _collector = collector;
    }

    public void nextTuple() {
        String uri = "hdfs://192.168.1.222:9000/storm/m3.txt";
        InputStream in = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            in = fs.open(new Path(uri));
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while (null != (line = br.readLine())) {
                _collector.emit(new Values(line));
                Utils.sleep(100);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ip"));
    }
}
