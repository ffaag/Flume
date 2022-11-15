package com.it.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ZuYingFang
 * @time 2022-04-17 17:55
 * @description
 */
public class MySink extends AbstractSink implements Configurable {

    // 创建logger对象
    private Logger logger = LoggerFactory.getLogger(MySink.class);
    private String prefix;
    private String suffix;


    //初始化 context（读取配置文件内容）
    @Override
    public void configure(Context context) {
        //读取配置文件内容，有默认值
        prefix = context.getString("prefix", "hello:");
        //读取配置文件内容，无默认值
        suffix = context.getString("suffix");
    }

    //从 Channel 读取获取数据（event），这个方法将被循环调用
    @Override
    public Status process() throws EventDeliveryException {

        // 获取channel并开启事务
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        // 从channel中抓取数据并打印到控制台
        try {
            // 抓取数据
            Event event;
            while (true){
                event = channel.take();
                if (event != null){
                    break;
                }
            }

            // 处理数据
            logger.info(prefix + new String(event.getBody()) + suffix);

            // 提交事务
            transaction.commit();
            return Status.READY;
        }catch (Exception e){
            // 回滚
            transaction.rollback();
            return Status.BACKOFF;
        }finally {
            transaction.close();
        }
    }
}
