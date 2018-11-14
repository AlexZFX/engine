//package com.alibabacloud.polar_race.engine.common;
//
//import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
//import org.junit.Assert;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.junit.runners.JUnit4;
//
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
///**
// * Author : Alex
// * Date : 2018/11/13 22:20
// * Description :
// */
//@RunWith(JUnit4.class)
//public class EngineTest {
//
//    //    private static final String RACE_PATH = "/Users/zhangfengxiao/race";
//    private static final String RACE_PATH = "C:\\idea";
//
//    private static final int WRITE_TIMES = 160000;
//
//    @Test
//    public void read() throws EngineException {
//        long start = System.currentTimeMillis();
//
//        EngineRace race = new EngineRace();
//        race.open(RACE_PATH);
//
//
//        for (int i = 0; i < WRITE_TIMES; i += 3) {
//            int finalI = i;
//            try {
//                byte[] bytes = race.read(Util.long2bytes(finalI));
//                long key = Util.bytes2long(bytes);
////                Assert.assertEquals(finalI, key);
//                if (finalI != key) {
//                    System.out.println("读取失败" + i);
////                    throw new EngineException(RetCodeEnum.IO_ERROR, "读取不匹配");
//                } else {
////                    System.out.println("读取成功" + i);
//                }
//            } catch (EngineException e) {
//                e.printStackTrace();
//            }
//        }
//
//        race.close();
//        long end = System.currentTimeMillis();
//        System.out.println(end - start);
//    }
//
//    @Test
//    public void write() throws EngineException {
//        long start = System.currentTimeMillis();
//
//        EngineRace race = new EngineRace();
//        race.open(RACE_PATH);
//
//
//        ExecutorService executor = Executors.newFixedThreadPool(64);
//        CountDownLatch countDownLatch = new CountDownLatch(WRITE_TIMES);
//        for (int i = 0; i < WRITE_TIMES; i++) {
//            int finalI = i;
//            executor.execute(() -> {
//                try {
//                    race.write(Util.long2bytes(finalI), Util.genvalue(finalI));
//                    countDownLatch.countDown();
//                } catch (EngineException e) {
//                    e.printStackTrace();
//                }
//            });
//        }
////        for (int i = 10000; i < 12000; i+=100) {
////            race.write(Util.long2bytes(i), Util.genvalue(i+1));
////        }
//        try {
//            countDownLatch.await();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        executor.shutdownNow();
//        race.close();
//        long end = System.currentTimeMillis();
//        System.out.println(end - start);
//    }
//
//
//    @Test
//    public void smallReadTest() throws EngineException {
//        EngineRace race = new EngineRace();
//
//        race.open(RACE_PATH);
//        for (int i = 7; i < 1000; i += 5) {
//            byte[] bytes = race.read(Util.long2bytes(i));
//            long key = Util.bytes2long(bytes);
//            if (i != key) {
//                System.out.println(i + " 读取不匹配 " + key);
//            } else {
//                System.out.println("读取成功");
//            }
//        }
//        race.close();
//    }
//
//    @Test
//    public void smallWrite() throws EngineException {
//        EngineRace race = new EngineRace();
//        race.open(RACE_PATH);
//
//        for (int i = 7; i < 1000; i++) {
//            int finalI = i;
//            race.write(Util.long2bytes(finalI), Util.genvalue(finalI));
//        }
//        race.close();
//    }
//
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
//
//}
