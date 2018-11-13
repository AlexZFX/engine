//package com.alibabacloud.polar_race.engine.common;
//
//import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.junit.runners.JUnit4;
//
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.Executor;
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
//    @Test
//    public void read() throws EngineException {
//        long start = System.currentTimeMillis();
//
//        EngineRace race = new EngineRace();
//        race.open("C:\\idea");
//
//
//        for (int i = 0; i < 64000; i += 6) {
//            int finalI = i;
//            try {
//                byte[] bytes = race.read(Util.long2bytes(finalI));
//                long key = Util.bytes2long(bytes);
//                if (i != key) {
//                    System.out.println(i + "读取不匹配");
//                } else {
//                    System.out.println("读取成功");
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
//        race.open("C:\\idea");
//
//
//        Executor executor = Executors.newFixedThreadPool(64);
//        CountDownLatch countDownLatch = new CountDownLatch(64000);
//        for (int i = 0; i < 64000; i++) {
//            int finalI = i;
//            executor.execute(new Runnable() {
//                @Override
//                public void run() {
//                    try {
//                        race.write(Util.long2bytes(finalI), Util.genvalue(finalI));
//                        countDownLatch.countDown();
//                    } catch (EngineException e) {
//                        e.printStackTrace();
//                    }
//                }
//            });
//        }
//
//        try {
//            countDownLatch.await();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        ((ExecutorService) executor).shutdownNow();
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
//        race.open("C:\\idea");
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
//        race.open("C:\\idea");
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
//        race.open("C:\\idea");
//
//
//        race.close();
//        long end = System.currentTimeMillis();
//        System.out.println(end - start);
//    }
//
//}
