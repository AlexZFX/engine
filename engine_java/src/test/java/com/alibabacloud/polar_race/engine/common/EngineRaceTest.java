package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Arrays;

/**
 * Author : Alex
 * Date : 2018/11/10 16:02
 * Description :
 */
@RunWith(JUnit4.class)
public class EngineRaceTest {

    @Test
    public void test() {
        EngineRace race = new EngineRace();
        try {
            race.open("F:\\idea");
            byte[] key1 = new byte[8];
            byte[] value1 = new byte[4096];
            byte[] key2 = new byte[8];
            byte[] value2 = new byte[4096];
            for (int i = 0; i < key1.length; i++) {
                key1[i] = (byte) i;
            }
            for (int i = 0; i < value1.length; i++) {
                value1[i] = (byte) i;
            }
            for (int i = 0; i < key2.length; i++) {
                key2[i] = key1[7 - i];
            }
            for (int i = 0; i < value2.length; i++) {
                value2[i] = 1;
            }
            for (int i = 0; i < 10; i++) {
                race.write(key1, value1);
                race.write(key2, value2);
            }
            System.out.println(Arrays.toString(key1));
            System.out.println(Arrays.toString(race.read(key1)));
            System.out.println(Arrays.toString(key2));
            System.out.println(Arrays.toString(race.read(key2)));
        } catch (EngineException e) {
            e.printStackTrace();
        }
    }

}