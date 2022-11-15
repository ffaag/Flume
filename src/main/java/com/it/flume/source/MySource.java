package com.it.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;


import java.util.HashMap;

/**
 * @author ZuYingFang
 * @time 2022-04-17 17:02
 * @description
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

    // 定义配置文件将来要读取的字段
    private Long delay;
    private String field;

    // 接收数据，将数据封装成一个个Event，写入Channel
    @Override
    public Status process() throws EventDeliveryException {

        // 创建事件头信息
        HashMap<String, String> headerMap = new HashMap<>();
        // 创建事件
        SimpleEvent event = new SimpleEvent();
        try {
            // 循环封装事件，往里面加数据
            for (int i = 0; i < 5; i++) {
                // 给事件设置头信息
                event.setHeaders(headerMap);
                // 给事件设置内容
                event.setBody((field + i).getBytes());
                // 将事件写入channel
                getChannelProcessor().processEvent(event);
            }
            Thread.sleep(delay);
            return Status.READY;
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
    }

    // backoff 步长
    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    // backoff 最长时间
    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


    // 读取配置文件信息
    @Override
    public void configure(Context context) {
        delay = context.getLong("delay");
        field = context.getString("field", "Hello!");
    }
}
