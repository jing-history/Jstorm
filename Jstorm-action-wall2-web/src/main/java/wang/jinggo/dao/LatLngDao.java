package wang.jinggo.dao;

import wang.jinggo.domain.LatLngBean;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wangyj
 * @description
 * @create 2018-09-02 16:54
 **/
public class LatLngDao {
    private static String url = "jdbc:mysql://192.168.1.222:3306/storm";
    private static String user = "hive";
    private static String password = "jinggo111";

    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url, user, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();

        } catch (SQLException e) {
            e.printStackTrace();

        }
        return conn;

    }

    public static List<LatLngBean> findAll() {

        Connection conn = getConnection();
        PreparedStatement ps = null;
        ResultSet rs = null;
        String sql = "select * from positions ";
        List<LatLngBean> list = new ArrayList<LatLngBean>();
        try {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                LatLngBean sto = new LatLngBean();
                ;
                sto.setLng(rs.getDouble("lng"));
                sto.setLat(rs.getDouble("lat"));
                sto.setAddress(rs.getString("AREA"));
                list.add(sto);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
    }
}
