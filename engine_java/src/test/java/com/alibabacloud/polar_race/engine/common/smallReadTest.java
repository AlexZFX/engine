/*package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class smallReadTest {

    @Test
    public void test() throws EngineException {
        EngineRace race = new EngineRace();

        race.open("C:\\idea");
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

}
*/