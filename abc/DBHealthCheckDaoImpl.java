package com.unbxd.feed.dao.impl;

import com.mongodb.*;
import com.unbxd.feed.cluster.model.ClusterInfo;
import com.unbxd.feed.cluster.service.ClusterConnService;
import com.unbxd.feed.cluster.service.dao.ClusterInfoDAO;
import com.unbxd.feed.dao.DBHealthCheckDao;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * /**
 * Created by charu on 12/7/17.
 */
@Service(value = "dBHealthCheckDao")
public class DBHealthCheckDaoImpl implements DBHealthCheckDao {

    @Resource(name = "clusterConnService")
    ClusterConnService clusterConnService;

    @Resource(name="clusterInfoDAO")
    ClusterInfoDAO clusterInfoDAO;

    private static final Logger LOGGER =
            Logger.getLogger(DBHealthCheckDaoImpl.class);

    public boolean checkDBHealth(String clusterName) {
        Mongo mongo = clusterConnService.getConnFromClusterName(clusterName);

        try {
            LOGGER.info("Trying to hit mongodb for clusterName: "+clusterName);
            DBObject ping = new BasicDBObject("ping", "1");
            mongo.getDB("dbname").command(ping);
            ClusterInfo ci = clusterInfoDAO.get("default");
            if(ci==null)
            {
                throw new Exception("couldn't find dbinstance '"+clusterName+"' in clusterInfo");
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("Health check error. Msg :" + e.getMessage(), e);
            return false;
        }

    }
}

