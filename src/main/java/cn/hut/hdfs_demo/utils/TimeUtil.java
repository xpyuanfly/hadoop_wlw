package cn.hut.hdfs_demo.utils;

import java.sql.Date;
import java.text.SimpleDateFormat;

public class TimeUtil {
    public static String longToDateString(long lo) {

        Date date = new Date(lo);

        // SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm");

        return sd.format(date);
    }

 }