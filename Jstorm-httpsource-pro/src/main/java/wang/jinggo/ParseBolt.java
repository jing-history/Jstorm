package wang.jinggo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author wangyj
 * @description
 * @create 2018-08-31 11:10
 **/
public class ParseBolt extends BaseRichBolt {
    private OutputCollector collector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=collector;
    }

    public void execute(Tuple tuple) {
// 这里只实现简单输出就好
        String word = tuple.getString(0);
        System.out.println(word);
        this.collector.ack(tuple);//告诉KafkaSpout已处理完成（必须应答Spout才记录读取进度）
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
