package wang.jinggo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author wangyj
 * @description
 * @create 2018-08-20 10:15
 **/
public class HotIPSplitBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        // 内容 1,201.105.101.102,http://mystore.jsp/?productid=1,2017020029,2,1
        String log = tuple.getString(0);

        //进行分词，解析出user_ip
        String[] words = log.split(",");

        //过滤不满足要求的日志信息
        if(words.length == 6){
            //System.out.println("解析的ip是：" + words[1]);
            //每个ip记一次数
            this.collector.emit(new Values(words[1],1));
        }
        this.collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //申明输出格式：两个字段（ip,1）
        outputFieldsDeclarer.declare(new Fields("ip", "count"));
    }
}
