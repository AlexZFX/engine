package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Author : Alex
 * Date : 2018/11/13 22:20
 * Description :
 */
@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.JVM)
public class EngineTest {

    //    private static final String RACE_PATH = "/Users/zhangfengxiao/race";
    private static final String RACE_PATH = "C:\\idea";

    private static final int WRITE_TIMES = 20000;

    //    @Test
//    public void openTest() throws EngineException {
//        long start = System.currentTimeMillis();
//
//        EngineRace race = new EngineRace();
//        race.open(RACE_PATH);
//
//
//        race.close();
//        long end = System.currentTimeMillis();
//        System.out.println(end - start);
//    }
    private static final long START = -9000000000000000000L;
    private static final long END = 9000000000000000000L;
    private static final long INTERVAL = END / WRITE_TIMES - START / WRITE_TIMES;

    @Test
    public void write() throws EngineException {
        long start = System.currentTimeMillis();
        System.out.println(INTERVAL);
        EngineRace race = new EngineRace();
        race.open(RACE_PATH);


        ExecutorService executor = Executors.newFixedThreadPool(64);
        CountDownLatch countDownLatch = new CountDownLatch(WRITE_TIMES);
        for (long i = START; i < END; i += INTERVAL) {
            long finalI = i;
            executor.execute(() -> {
                try {
                    race.write(Util.long2bytes(finalI), Util.genvalue(finalI));
                    countDownLatch.countDown();
                } catch (EngineException e) {
                    e.printStackTrace();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        for (int i = 10000; i < 12000; i += 100) {
        for (long i = START + 10000 * INTERVAL; i < START + 12000 * INTERVAL; i += 100 * INTERVAL) {
            race.write(Util.long2bytes(i), Util.genvalue(i + 1));
        }
        executor.shutdownNow();
        race.close();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }


    @Test
    public void read() throws EngineException {
        long start = System.currentTimeMillis();

        EngineRace race = new EngineRace();
        race.open(RACE_PATH);

//        for (int i = 0; i < WRITE_TIMES; i++) {
        for (long i = START; i < END; i += INTERVAL) {
            long finalI = i;
            try {
                byte[] bytes = race.read(Util.long2bytes(finalI));
                long key = Util.bytes2long(bytes);
//                if (finalI >= 10000 && finalI < 12000 && finalI % 100 == 0) {
                if (finalI >= START + 10000 * INTERVAL && finalI < START + 12000 * INTERVAL && (i / INTERVAL - START / INTERVAL) % 100 == 0) {
//                    System.out.println("重复key:" + finalI);
                    if (finalI + 1 != key) {
                        System.out.println(finalI + "匹配错误,key:" + key);
                    }
//                    Assert.assertEquals(finalI + 1, key);
                } else {
//                    Assert.assertEquals(finalI, key);
//                }
                    if (finalI != key) {
                        System.out.println(finalI + "匹配错误,key:" + key);
//                    throw new EngineException(RetCodeEnum.IO_ERROR, "读取不匹配");
                    } else {
//                        System.out.println("读取成功" + i);
                    }
                }
            } catch (EngineException e) {
                e.printStackTrace();
            }
        }

        race.close();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

    @Test
    public void smallReadTest() throws EngineException {
        EngineRace race = new EngineRace();

        race.open(RACE_PATH);
        for (int i = 7; i < 1000; i += 5) {
            byte[] bytes = race.read(Util.long2bytes(i));
            long key = Util.bytes2long(bytes);
            if (i != key) {
                System.out.println(i + " 读取不匹配 " + key);
            } else {
                System.out.println("读取成功");
            }
        }
        race.close();
    }

    @Test
    public void smallWrite() throws EngineException {
        EngineRace race = new EngineRace();
        race.open(RACE_PATH);

        for (int i = 0; i < 1000; i++) {
            int finalI = i;
            race.write(Util.long2bytes(finalI), Util.genvalue(finalI));
        }
        race.close();
    }

    @Test
    public void smallSameWrite() throws EngineException {
        EngineRace race = new EngineRace();
        race.open(RACE_PATH);

        for (int i = 0; i < 1000; i++) {
            int finalI = i;
            race.write(Util.long2bytes(1), Util.genvalue(finalI));
        }
        race.close();
    }

    @Test
    public void smallSameReadTest() throws EngineException {
        EngineRace race = new EngineRace();

        race.open(RACE_PATH);
        for (int i = 7; i < 1000; i += 5) {
            byte[] bytes = race.read(Util.long2bytes(1));
            long key = Util.bytes2long(bytes);
            if (999 != key) {
                System.out.println(i + " 读取不匹配 " + key);
            } else {
                System.out.println("读取成功");
            }
        }
        race.close();
    }

    @Test
    public void rangeTest() throws Exception {
        long start = System.currentTimeMillis();

        EngineRace race = new EngineRace();
        race.open(RACE_PATH);

        System.out.println("open end");
        ExecutorService executor = Executors.newFixedThreadPool(64);
        CountDownLatch countDownLatch = new CountDownLatch(64);
        for (int i = 0; i < 64; i++) {
            int finalI = i;
            executor.execute(() -> {
                try {
//                    System.out.println("-----rangetest begin-----");
                    race.range(null, null, new TestVisitor());
                    race.range(null, null, new TestVisitor());
//                    System.out.println("-----rangetest end-----");
                    countDownLatch.countDown();
                } catch (EngineException e) {
                    e.printStackTrace();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executor.shutdownNow();
        race.close();
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

}
