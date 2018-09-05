package wang.jinggo.service;

import wang.jinggo.dao.LatLngDao;
import wang.jinggo.domain.LatLngBean;

import java.util.List;

/**
 * @author wangyj
 * @description
 * @create 2018-09-02 16:54
 **/
public class StormService {
    public List<LatLngBean> findAll(){
        return LatLngDao.findAll();
    }
}
