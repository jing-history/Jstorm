package wang.jinggo.domain;

/**
 * @author wangyj
 * @description
 * @create 2018-09-02 16:53
 **/
public class LatLngBean {

    private  double lng;
    private  double lat;
    private  String  address;
    public LatLngBean(){

    }
    public double getLng() {
        return lng;
    }
    public void setLng(double lng) {
        this.lng = lng;
    }
    public double getLat() {
        return lat;
    }
    public void setLat(double lat) {
        this.lat = lat;
    }
    public String getAddress() {
        return address;
    }
    public void setAddress(String address) {
        this.address = address;
    }
}
