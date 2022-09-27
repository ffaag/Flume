package com.it.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author ZuYingFang
 * @time 2022-04-17 16:21
 * @description
 */
public class CustomInterceptor implements Interceptor {

    // 存放事件的集合
    private List<Event> addHeaderEvents;

    @Override
    public void initialize() {
        addHeaderEvents = new ArrayList<>();
    }

    // 单个事件拦截
    @Override
    public Event intercept(Event event) {
        // 首先获得事件的头部信息
        Map<String, String> headers = event.getHeaders();
        // 获得事件的body信息
        String body = new String(event.getBody());
        // 如果body中含有xiaofang，则将其head的key为type对应的value为first，这样在flume的配置文件中得到first就将其输送到指定的channel中
        if (body.contains("xiaofang")) {
            headers.put("type", "first");
        } else {
            headers.put("type", "second");
        }
        return event;
    }

    // 批量事件拦截
    @Override
    public List<Event> intercept(List<Event> list) {
        // 将事件集合清空，再添加我们参数带来的批量事件
        addHeaderEvents.clear();
        for (Event event : list) {
            addHeaderEvents.add(event);
        }

        return addHeaderEvents;
    }

    @Override
    public void close() {

    }


    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new CustomInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
