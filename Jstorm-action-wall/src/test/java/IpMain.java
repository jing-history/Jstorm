/**
 * @author wangyj
 * @description
 * @create 2018-09-04 15:12
 **/
public class IpMain {

    public static void main(String[] args) {
            String ipa = "162.117.71.45";
        System.out.println(ipToLong(ipa));
    }

    public static long ipToLong(String strIp) {
        // transfer ip like 127.0.0.1 to decimal integer
        //1413276006	18540852316	71-77-16-4c-41-b4:CMCC	10.116.136.202	alipay.com	支付	15	9	7161	4269	200
        long[] ip = new long[4];
        // find the position of dot
        int position1 = strIp.indexOf(".");
        int position2 = strIp.indexOf(".", position1 + 1);
        int position3 = strIp.indexOf(".", position2 + 1);
        // transfer string to integer
        ip[0] = Long.parseLong(strIp.substring(0, position1));
        ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));
        ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));
        ip[3] = Long.parseLong(strIp.substring(position3 + 1));
        return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
    }
}
