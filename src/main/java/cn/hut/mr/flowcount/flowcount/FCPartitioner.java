package cn.hut.mr.flowcount.flowcount;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import cn.hut.mr.flowcount.bean.FlowBean;

public class FCPartitioner extends Partitioner<Text, FlowBean> {

    private static Map<String, Integer> rules = new HashMap<String, Integer>();
    // public static Map getRules(){
    // return rules;
    // }
    static {
        rules.put("134", 4);
        rules.put("135", 5);
        rules.put("136", 6);
        rules.put("137", 7);
        rules.put("138", 8);
        rules.put("139", 9);
    }

    @Override
    public int getPartition(Text k2, FlowBean v2, int numPartitions) {
        // <"13726230503", bean>
        String pre_telno = k2.toString().substring(0, 3);
        Integer res = rules.get(pre_telno);
        if (res==null)
            res=0;  // 默认分区，其他分区     
        return res;
    }

}