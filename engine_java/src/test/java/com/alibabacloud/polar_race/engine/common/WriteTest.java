package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created on 2018/11/13.
 */
@RunWith(JUnit4.class)
public class WriteTest {

    @Test
    public void write() throws EngineException {
        long start = System.currentTimeMillis();

        EngineRace race = new EngineRace();
        race.open("C:\\idea");


        Executor executor = Executors.newFixedThreadPool(64);
        CountDownLatch countDownLatch = new CountDownLatch(64000);
        for (int i = 0; i < 64000; i++) {
            int finalI = i;
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        race.write(Util.long2bytes(finalI),Util.genvalue(finalI));
                        countDownLatch.countDown();
                    } catch (EngineException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ((ExecutorService) executor).shutdownNow();
        race.close();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }


}
