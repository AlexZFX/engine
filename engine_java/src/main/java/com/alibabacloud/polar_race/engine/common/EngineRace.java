package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;
import io.netty.util.concurrent.FastThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class EngineRace extends AbstractEngine {

    private static Logger logger = LoggerFactory.getLogger(EngineRace.class);
    // key+offset 长度 16B
    private static final int KEY_AND_OFF_LEN = 12;
    // 线程数量
    private static final int THREAD_NUM = 64;
    // value 长度 4K
    private static final int VALUE_LEN = 4096;
    //每个map存储的key数量
    private static final int PER_MAP_COUNT = 1024000;

    private static final int SHIFT_NUM = 12;
    //    存放 value 的文件数量 128
    private static final int FILE_COUNT = 64;

    private static final int HASH_VALUE = 0x3F;

    private static final LongIntHashMap[] keyMap = new LongIntHashMap[THREAD_NUM];

    static {
        for (int i = 0; i < THREAD_NUM; i++) {
            keyMap[i] = new LongIntHashMap(PER_MAP_COUNT, 0.98);
        }
    }

    //key 文件的fileChannel
    private static FileChannel[] keyFileChannels = new FileChannel[THREAD_NUM];

    private static AtomicInteger[] keyOffsets = new AtomicInteger[THREAD_NUM];

    private static MappedByteBuffer[] keyMappedByteBuffers = new MappedByteBuffer[THREAD_NUM];

    //value 文件的fileChannel
    private static FileChannel[] fileChannels = new FileChannel[FILE_COUNT];

    private static AtomicInteger[] valueOffsets = new AtomicInteger[FILE_COUNT];

    private static FastThreadLocal<ByteBuffer> localBufferValue = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocate(VALUE_LEN);
        }
    };

    @Override
    public void open(String path) throws EngineException {
        File file = new File(path);
        // 创建目录
        if (!file.exists()) {
            if (!file.mkdir()) {
                throw new EngineException(RetCodeEnum.IO_ERROR, "创建文件目录失败：" + path);
            } else {
                logger.info("创建文件目录成功：" + path);
            }
        }
        RandomAccessFile randomAccessFile;
        // file是一个目录时进行接下来的操作
        if (file.isDirectory()) {
            try {
                //先 创建 FILE_COUNT个FileChannel 供write顺序写入，并由此文件获取value文件的大小
                for (int i = 0; i < FILE_COUNT; i++) {
                    try {
                        randomAccessFile = new RandomAccessFile(path + File.separator + i + ".data", "rw");
                        FileChannel channel = randomAccessFile.getChannel();
                        fileChannels[i] = channel;
                        // 从 length处直接写入
                        valueOffsets[i] = new AtomicInteger((int) (randomAccessFile.length() >>> SHIFT_NUM));
                        keyOffsets[i] = new AtomicInteger(valueOffsets[i].get() * KEY_AND_OFF_LEN);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                //先构建keyFileChannel 和 初始化 map
                for (int i = 0; i < THREAD_NUM; i++) {
                    randomAccessFile = new RandomAccessFile(path + File.separator + i + ".key", "rw");
                    FileChannel channel = randomAccessFile.getChannel();
                    keyFileChannels[i] = channel;
                    keyMappedByteBuffers[i] = channel.map(FileChannel.MapMode.READ_WRITE, 0, PER_MAP_COUNT * 20);
                }
                CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
                for (int i = 0; i < THREAD_NUM; i++) {
                    if (!(keyOffsets[i].get() == 0)) {
                        final long off = keyOffsets[i].get();
                        final int finalI = i;
                        final MappedByteBuffer buffer = keyMappedByteBuffers[i];
                        new Thread(() -> {
                            int start = 0;
                            while (start < off) {
                                start += KEY_AND_OFF_LEN;
                                keyMap[finalI].put(buffer.getLong(), buffer.getInt());
                            }
                            countDownLatch.countDown();
                        }).start();
                    } else {
                        countDownLatch.countDown();
                    }
                }
                countDownLatch.await();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            throw new EngineException(RetCodeEnum.IO_ERROR, "path不是一个目录");
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        long numkey = Util.bytes2long(key);
        int hash = valueFileHash(numkey);
        int off = valueOffsets[hash].getAndIncrement();
        try {
            ByteBuffer keyBuffer = keyMappedByteBuffers[hash].slice();
            keyBuffer.position(keyOffsets[hash].getAndAdd(KEY_AND_OFF_LEN));
            keyBuffer.putLong(numkey).putInt(off);
            //将value写入buffer
            ByteBuffer valueBuffer = localBufferValue.get();
            valueBuffer.clear();
            valueBuffer.put(value);
            valueBuffer.flip();
            fileChannels[hash].write(valueBuffer, ((long) off) << SHIFT_NUM);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "写入数据出错");
        }
    }


    @Override
    public byte[] read(byte[] key) throws EngineException {
        long numkey = Util.bytes2long(key);
        int hash = valueFileHash(numkey);
        long off = keyMap[hash].getOrDefault(numkey, -1);
        ByteBuffer buffer = localBufferValue.get();
        if (off == -1) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, numkey + "不存在");
        }
        try {
            buffer.clear();
            fileChannels[hash].read(buffer, off << SHIFT_NUM);
        } catch (IOException e) {
            throw new EngineException(RetCodeEnum.IO_ERROR, "读取数据出错");
        }
        return buffer.array();
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
    }

    @Override
    public void close() {
        for (int i = 0; i < FILE_COUNT; i++) {
            try {
                keyFileChannels[i].close();
                fileChannels[i].close();
            } catch (IOException e) {
                logger.error("close error");
            }
        }
    }

    private static int valueFileHash(long key) {
        return (int) (key & HASH_VALUE);
    }

}
