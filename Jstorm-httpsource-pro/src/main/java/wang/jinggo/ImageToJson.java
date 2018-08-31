package wang.jinggo;

import com.google.gson.Gson;
import org.apache.flume.event.JSONEvent;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author wangyj
 * @description
 * @create 2018-08-31 11:13
 **/
public class ImageToJson {

    public static void main(String[] args) {
        JSONEvent jse = new JSONEvent();
        jse.setBody("this is a message, hahhahhahhahhahah".getBytes());
        Gson gson = new Gson();
        List<Object> events1 = new ArrayList<Object>();
        events1.add(jse);
        String jsonstr = gson.toJson(events1);
        post("http://192.168.1.22:50000", jsonstr);
    }

    public static void post(String urlstr, String json){
        try{
            //创建连接
            URL url = new URL(urlstr);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("POST");
            connection.setUseCaches(false);
            connection.setInstanceFollowRedirects(true);
            connection.setRequestProperty("Content-Type",
                    "application/x-www-form-urlencoded");
            connection.connect();
            //POST请求
            DataOutputStream out = new DataOutputStream(
                    connection.getOutputStream());
            out.writeBytes(json);
            out.flush();
            out.close();
            //读取响应
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String lines;
            StringBuffer sb = new StringBuffer("");
            while ((lines = reader.readLine()) != null) {
                lines = new String(lines.getBytes(), "utf-8");
                sb.append(lines);
            }
            System.out.println(sb);
            reader.close();
            // 断开连接
            connection.disconnect();
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
