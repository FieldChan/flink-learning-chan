package com.chan.common.data;

import com.chan.common.entity.Order;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class OrderGenerator extends RichParallelSourceFunction<Order> {

    private static final Random random = new Random();

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static long orderId = 0;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while (true) {
            TimeUnit.MILLISECONDS.sleep(2000);

            int cityId = random.nextInt(10);
            if(cityId == 0){
                sourceContext.collect(null);
            }

            Order order = Order.builder()
                    .createTime(System.currentTimeMillis())
                    .orderCreateTime(sdf.format(System.currentTimeMillis()))
                    .orderId("orderId:1")
//                    .orderId("orderId:" + random.nextInt(100))
                    .cityId(cityId)
                    .goodsId(random.nextInt(10))
                    .price(random.nextInt(10000))
                    .userId("UserId:" + random.nextInt(10000))
                    .build();
            sourceContext.collect(order);
        }
    }

    @Override
    public void cancel() {

    }
}
