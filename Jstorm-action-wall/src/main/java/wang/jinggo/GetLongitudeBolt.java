package wang.jinggo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangyj
 * @description
 * @create 2018-09-02 16:31
 **/
public class GetLongitudeBolt implements IBasicBolt {

    private static final long serialVersionUID = 1L;
    private HashMap<String, String> longitude = new HashMap<String, String>();
    static Connection conn;
    static Statement st;

    private String uri = "hdfs://192.168.1.222:9000/storm/lng-lat-mapping.txt";

    public static Connection getConnection() {
        Connection con = null; // get connection
        try {
            Class.forName("com.mysql.jdbc.Driver");// load Mysql driver
            con = DriverManager.getConnection(
                    "jdbc:mysql://192.168.1.222:3306/storm", "hive", "jinggo111");
        } catch (Exception e) {
            System.out.println("connect mysql failed! " + e.getMessage());
            if(conn != null){
                try {
                    con.close();
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return con; // return connection
    }

    public static void insert(String area, String jing, String wei) {
        conn = getConnection(); // get connection
        try {
            String sql = "INSERT INTO positions(area,lng,lat)" + " VALUES ('"
                    + area + "','" + jing + "','" + wei + "')";
            st = (Statement) conn.createStatement(); // create static sql statement
            st.executeUpdate(sql); // exec sql
            conn.close(); // close connection
        } catch (SQLException e) {
            System.out.println("insert failed! " + e.getMessage());
        }
    }

    public void prepare(Map map, TopologyContext topologyContext) {
        conn = getConnection();
        try {
            st = (Statement) conn.createStatement();
        } catch (SQLException e1) {
            e1.printStackTrace();
        }

        InputStream in = null;
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(uri), new Configuration());
            in = fs.open(new Path(uri));
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        try {
            while (null != (line = br.readLine())) {
                longitude.put(line.split("\t", -1)[0], line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    //    String word = tuple.toString();
        String word = tuple.getStringByField("area");
        System.out.println("GetBolt==========================>>>> " + longitude.get(word));
        if (longitude.get(word) != null) {
            insert(longitude.get(word).split("\t", -1)[0], longitude.get(word)
                    .split("\t", -1)[1], longitude.get(word).split("\t", -1)[2]);
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("area", "lng", "lat"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
