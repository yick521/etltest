package com.zhugeio.etl.pipeline.operator.id;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.zhugeio.etl.pipeline.example.ZGMessage;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class SessionIdAsyncOperator extends RichAsyncFunction<ZGMessage, ZGMessage> {

    @Override
    public void asyncInvoke(ZGMessage input, ResultFuture<ZGMessage> resultFuture) {
        if(input.getResult() != -1){
            CompletableFuture.supplyAsync(() -> createOutput(input)).whenComplete((output, throwable) -> {
                if (throwable != null) {
                    resultFuture.completeExceptionally(throwable);
                } else {
                    resultFuture.complete(Collections.singleton(output));
                }
            });
        }else {
            resultFuture.complete(Collections.singleton(input));
        }

    }

    private ZGMessage createOutput(ZGMessage input) {
        JSONArray data = (JSONArray)input.getData();
        for (int i = 0; i < data.size(); i++){
            JSONObject item = data.getJSONObject(i);
            if (item != null) {
                Object pr = item.get("pr");
                if (pr instanceof JSONObject) {
                    JSONObject prObject = (JSONObject) pr;
                    if(prObject.containsKey("$sid")){
                        prObject.put("$zg_sid", prObject.getLongValue("$sid"));
                    }else {
                        prObject.put("$zg_sid", -1L);
                    }
                }
                String dt = item.getString("dt");
                if("evt".equals(dt) || "ss".equals(dt) || "se".equals(dt) || "mkt".equals(dt) || "abp".equals(dt)){
                    item.put("$uuid", generateUUID());
                }
            }
        }
        return input;
    }

    private String generateUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }
}