package bolt;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * @author wangyj
 * @description
 * @create 2018-08-20 11:09
 **/
public class WordCountBolt extends BaseRichBolt {

    private static Logger logger = Logger.getLogger(WordCountBolt.class);

    private Connection connection;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        connect();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        String sql = "insert into t_word (word) values ('" + word + "')";
        Statement stat = null;
        try {
            stat = this.connection.createStatement();
            stat.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private Connection connect() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            logger.error(e);
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:mysql://192.168.1.222:3306/storm",
                            "hive", "jinggo111");
            this.connection = conn;
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e);
        }
        return conn;
    }
}
