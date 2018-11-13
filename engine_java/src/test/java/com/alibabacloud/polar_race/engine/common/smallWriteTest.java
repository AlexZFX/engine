package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Author : Alex
 * Date : 2018/11/10 16:02
 * Description :
 */
@RunWith(JUnit4.class)
public class smallWriteTest {

    @Test
    public void test() throws EngineException {
        EngineRace race = new EngineRace();
        race.open("C:\\idea");

        for (int i = 7; i < 1000; i ++) {
            int finalI = i;
            race.write(Util.long2bytes(finalI), Util.genvalue(finalI));
        }
        race.close();
    }

}