package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import io.netty.util.concurrent.FastThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class EngineRace extends AbstractEngine {

    private static Logger logger = LoggerFactory.getLogger(EngineRace.class);

    private static final int KEY_LEN = 8;
    //总key数量
    private static final int KEY_NUM = 64000000;
    //    private static final int KEY_NUM = 64000;
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
    // 第i个key
    private static final long[] keys = new long[KEY_NUM];
    // 第i个key的对应value的索引
    private static final int[] offs = new int[KEY_NUM];

    //key 文件的fileChannel
    private static FileChannel[] keyFileChannels = new FileChannel[THREAD_NUM];

    private static AtomicInteger[] keyOffsets = new AtomicInteger[THREAD_NUM];

    private static MappedByteBuffer[] keyMappedByteBuffers = new MappedByteBuffer[THREAD_NUM];

    //value 文件的fileChannel
    private static FileChannel[] fileChannels = new FileChannel[FILE_COUNT];
    //每个valueOffsets表示的都是第i个文件中的value数量
    private static AtomicInteger[] valueOffsets = new AtomicInteger[FILE_COUNT];

    private static FastThreadLocal<ByteBuffer> localBufferValue = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocate(VALUE_LEN);
        }
    };

    private static FastThreadLocal<byte[]> localKeyBytes = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() throws Exception {
            return new byte[KEY_LEN];
        }
    };

    private int CURRENT_KEY_NUM;

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
                CURRENT_KEY_NUM = 0;
                for (int i = 0; i < THREAD_NUM; i++) {
                    if (!(keyOffsets[i].get() == 0)) {
                        final long off = keyOffsets[i].get();
                        // 第i个文件写入 keys 的起始位置
                        final int temp = CURRENT_KEY_NUM;
                        CURRENT_KEY_NUM += valueOffsets[i].get();
                        final MappedByteBuffer buffer = keyMappedByteBuffers[i];
                        new Thread(() -> {
                            int start = 0;
                            int n = temp;
                            while (start < off) {
                                start += KEY_AND_OFF_LEN;
                                keys[n] = buffer.getLong();
                                offs[n++] = buffer.getInt();
                            }
                            countDownLatch.countDown();
                        }).start();
                    } else {
                        countDownLatch.countDown();
                    }
                }
                countDownLatch.await();
                //获取完之后对key进行排序
                heapSort(CURRENT_KEY_NUM);
                handleDuplicate(CURRENT_KEY_NUM);
                logger.error("CURRENT_KEY_NUM = " + CURRENT_KEY_NUM);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            throw new EngineException(RetCodeEnum.IO_ERROR, "path不是一个目录");
        }
    }

    private void handleDuplicate(int keyNum) {
        int max, start;
        for (int i = 0; i < keyNum; ++i) {
            start = i;
            max = offs[i];
            while (i + 1 < keyNum && keys[i] == keys[i + 1]) {
                max = Math.max(max, offs[i + 1]);
                ++i;
            }
            while (start <= i) {
                offs[start] = max;
                ++start;
            }
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
        long off = getKey(numkey);
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
        long key;
        int hash;
        ByteBuffer buffer = localBufferValue.get();
        byte[] bytes = localKeyBytes.get();
        logger.info("CURRENT_KEY_NUM = " + CURRENT_KEY_NUM);
        if ((lower == null || lower.length < 1) && (upper == null || upper.length < 1)) {
            try {
                for (int i = 0; i < CURRENT_KEY_NUM; ++i) {
                    while (i + 1 < CURRENT_KEY_NUM && keys[i] == keys[i + 1]) {
                        ++i;
                    }
                    key = keys[i];
                    hash = valueFileHash(key);
                    buffer.clear();
                    fileChannels[hash].read(buffer, offs[i] << SHIFT_NUM);
                    long2bytes(bytes, key);
                    logger.info(i + ":" + Arrays.toString(bytes) + "-" + key);
                    visitor.visit(bytes, buffer.array());
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new EngineException(RetCodeEnum.IO_ERROR, "range read io 出错");
            }
        } else {
            throw new EngineException(RetCodeEnum.NOT_SUPPORTED, "range传入的lower，upper 不为空");
        }
    }

    private void long2bytes(byte[] bytes, long key) {
        for (int i = 7; i >= 0; i--) {
            bytes[i] = (byte) (key & 0xFF);
            key >>= 8;
        }
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

    private int getKey(long numkey) {
        int l = 0, r = CURRENT_KEY_NUM - 1, mid;
        long num;
        while (l <= r) {
            mid = (l + r) >> 1;
            num = keys[mid];
            if (num < numkey) {
                l = mid + 1;
            } else if (num > numkey) {
                r = mid - 1;
            } else {
                return offs[mid];
            }
        }
        return -1;
    }

    /**
     * 对 index数组进行堆排
     *
     * @param startKeyNum 只排序前startKeyNum个数字
     */
    private void heapSort(int startKeyNum) {
        int end = startKeyNum - 1;
        for (int i = end >> 1; i >= 0; --i) {
            shiftDown(end, i);
        }
        for (int keyNum = end; keyNum > 0; --keyNum) {
            swap(keyNum, 0);
            shiftDown(keyNum - 1, 0);
        }
        for (int i = 0; i < 500; i++) {
            logger.error("heapSort 结果 ，第i位数字为 " + keys[i]);
        }
    }

    /**
     * index是从 0 开始的数组，则 k *2 之后要 + 1
     *
     * @param end 待排序的数组末尾
     * @param k   待shiftDown的位置
     */
    private void shiftDown(int end, int k) {
        int j = (k << 1) + 1;
        while (j <= end) {
            // 比较的数字是 index对应的key
            if (j + 1 <= end && keys[j] < keys[j + 1]) {
                ++j;
            }
            if (keys[k] >= keys[j]) {
                break;
            }
            swap(k, j);
            k = j;
            j = (k << 1) + 1;
        }
    }

    private void swap(int i, int j) {
        long temp = keys[i];
        keys[i] = keys[j];
        keys[j] = temp;
        temp = offs[i];
        offs[i] = offs[j];
        offs[j] = (int) temp;
    }

}
