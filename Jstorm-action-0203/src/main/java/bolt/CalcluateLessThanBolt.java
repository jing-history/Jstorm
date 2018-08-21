package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import utils.CalculateCache;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wangyj
 * @description
 * @create 2018-08-21 14:50
 **/
public class CalcluateLessThanBolt extends BaseRichBolt {

    private OutputCollector collector;
    CalculateCache cache;

    //设定超时时间
    int timeSection;
    //计算线程描述
    String desc;

    public CalcluateLessThanBolt(int timeSection, String desc){
        this.timeSection = timeSection;
        this.desc = desc;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        cache = new CalculateCache(timeSection, new CalcluateLessThanBoltExpiredCallback());
    }

    public void execute(Tuple tuple) {
        cache.put(tuple.getIntegerByField("lt"));
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    private class CalcluateLessThanBoltExpiredCallback implements CalculateCache.ExpiredCallback{

        public void expire(AtomicInteger currentValue) {
            int finalResult = currentValue.getAndSet(0);
            System.out.printf("%s的%d分钟内数据LESS个数为%d; %s\n", new Object[]{desc, timeSection, finalResult, System.currentTimeMillis()/1000});
        }

    }
}
