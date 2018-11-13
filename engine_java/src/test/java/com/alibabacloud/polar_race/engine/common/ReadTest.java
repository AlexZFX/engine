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
public class ReadTest {

    @Test
    public void read() throws EngineException {
        long start = System.currentTimeMillis();

        EngineRace race = new EngineRace();
        race.open("C:\\idea");


        for (int i = 7; i < 6400; i += 1000) {
            int finalI = i;
            try {
                byte[] bytes = race.read(Util.long2bytes(finalI));
                long key = Util.bytes2long(bytes);
                if (i != key) {
                    System.out.println(i + "读取不匹配");
                }else {
                    System.out.println("读取成功");
                }
            } catch (EngineException e) {
                e.printStackTrace();
            }
        }

        race.close();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

}
