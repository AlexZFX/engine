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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
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

    private static final int SHIFT_NUM = 12;
    // 存放 value 的文件数量 128
    private static final int FILE_COUNT = 512;

    //128块 1块 8.4375m = 8640 KB = 8847360 B  1个文件 4320m
    private static final int VALUE_FILE_SIZE = 520 * 1024 * 1024;

//    private static final long BLOCK_SIZE = 8847360L;
    // BLOCK_SIZE / VALUE_LEN
//    private static final int MAX_NUM_PER_BLOCK = 2160;

    //    private static final int HASH_VALUE = 0x1FF;
    // 第i个key
//    private static final long[] keys = new long[KEY_NUM];
    private static long[] keys;
    // 第i个key的对应value的索引
//    private static final int[] offs = new int[KEY_NUM];
    private static int[] offs;

    private static LongIntHashMap[] map = new LongIntHashMap[THREAD_NUM];

    //key 文件的fileChannel
    private static FileChannel[] keyFileChannels = new FileChannel[THREAD_NUM];

    private static AtomicInteger[] keyOffsets = new AtomicInteger[THREAD_NUM];

//    private static MappedByteBuffer[] keyMappedByteBuffers = new MappedByteBuffer[THREAD_NUM];

    //value 文件的fileChannel
    private static FileChannel[] fileChannels = new FileChannel[FILE_COUNT];
    //每个valueOffsets表示的都是第i个文件中的value数量
    private static AtomicInteger[] valueOffsets = new AtomicInteger[FILE_COUNT];

    // 一大块共享缓存
    private volatile ByteBuffer sharedBuffer;

    private static ByteBuffer[] caches = new ByteBuffer[2];

    static {
        caches[0] = ByteBuffer.allocateDirect(VALUE_FILE_SIZE);
        caches[1] = ByteBuffer.allocateDirect(VALUE_FILE_SIZE);
    }

    private static final List<Thread> threadList = new ArrayList<>(THREAD_NUM);

    private volatile boolean isFirst = true;
    //初始设置为 1 跳过第一块
    private volatile int fileReadCount = 1;

    private static ExecutorService executors = Executors.newSingleThreadExecutor();

    private final CyclicBarrier cyclicBarrier = new CyclicBarrier(THREAD_NUM, new Runnable() {
        @Override
        public void run() {
            if (fileReadCount < 512) {
                if (isFirst) {
                    sharedBuffer = caches[0];
                    executors.execute(() -> {
                        try {
                            caches[1].clear();
                            fileChannels[fileReadCount].read(caches[1]);
//                            cyclicBarrier.reset();
//                            for (Thread thread : threadList) {
//                                LockSupport.unpark(thread);
//                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                } else {
                    sharedBuffer = caches[1];
                    executors.execute(() -> {
                        try {
                            caches[0].clear();
                            fileChannels[fileReadCount].read(caches[0]);
//                            cyclicBarrier.reset();
//                            for (Thread thread : threadList) {
//                                LockSupport.unpark(thread);
//                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                }
                ++fileReadCount;
                isFirst = !isFirst;
            }
        }
    });


    private static FastThreadLocal<ByteBuffer> localBufferKey = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocate(KEY_AND_OFF_LEN);
        }
    };

    private static FastThreadLocal<ByteBuffer> localBufferValue = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocateDirect(VALUE_LEN);
        }
    };

    private static FastThreadLocal<byte[]> localKeyBytes = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() throws Exception {
            return new byte[KEY_LEN];
        }
    };

    private static FastThreadLocal<byte[]> localValueBytes = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() throws Exception {
            return new byte[VALUE_LEN];
        }
    };

    private int CURRENT_KEY_NUM;


    @Override
    public void open(String path) throws EngineException {
        logger.info("--------open--------");
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
                //先构建keyFileChannel 和 初始化 map
                for (int i = 0; i < THREAD_NUM; i++) {
                    randomAccessFile = new RandomAccessFile(path + File.separator + i + ".key", "rw");
                    FileChannel channel = randomAccessFile.getChannel();
                    keyFileChannels[i] = channel;
                    keyOffsets[i] = new AtomicInteger((int) channel.size());
                }
                CountDownLatch countDownLatch = new CountDownLatch(THREAD_NUM);
                CURRENT_KEY_NUM = 0;
                for (int i = 0; i < THREAD_NUM; i++) {
                    if (!(keyOffsets[i].get() == 0)) {
                        if (keys == null) {
                            keys = new long[KEY_NUM];
                            offs = new int[KEY_NUM];
                        }
                        final long off = keyOffsets[i].get();
                        logger.info("第" + i + "个key文件的大小为 ：" + (off / 1024) + "kB");
                        // 第i个文件写入 keys 的起始位置
                        final int temp = CURRENT_KEY_NUM;
                        CURRENT_KEY_NUM += off / KEY_AND_OFF_LEN;
                        final MappedByteBuffer buffer = keyFileChannels[i].map(FileChannel.MapMode.READ_ONLY, 0, keyOffsets[i].get());
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
                if (keys == null) {
                    for (int i = 0; i < THREAD_NUM; i++) {
                        map[i] = new LongIntHashMap(1024000, 0.99);
                    }
                }

                for (int i = 0; i < FILE_COUNT; i++) {
                    try {
                        randomAccessFile = new RandomAccessFile(path + File.separator + i + ".data", "rw");
                        FileChannel channel = randomAccessFile.getChannel();
                        fileChannels[i] = channel;
                        valueOffsets[i] = new AtomicInteger((int) (channel.size() >>> SHIFT_NUM));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                // 对range时的第一块进行初始化
                fileChannels[0].read(caches[0]);
                //获取完之后对key进行排序

                long sortStartTime = System.currentTimeMillis();
                heapSort(CURRENT_KEY_NUM);
                long sortEndTime = System.currentTimeMillis();
                logger.info("sort 耗时 " + (sortEndTime - sortStartTime) + "ms");
                logger.info("CURRENT_KEY_NUM = " + CURRENT_KEY_NUM);
//                CURRENT_KEY_NUM = handleDuplicate(CURRENT_KEY_NUM);
//                logger.info("handleDuplicate 耗时" + (System.currentTimeMillis() - sortEndTime) + "ms");
//                logger.info("CURRENT_KEY_NUM is " + CURRENT_KEY_NUM + " after handle duplicate");
                //创建 FILE_COUNT个FileChannel 分块写入
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            throw new EngineException(RetCodeEnum.IO_ERROR, "path不是一个目录");
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        long numkey = Util.bytes2long(key);
        int fileHash = valueFileHash(numkey);
        int keyHash = fileHash >>> 3;
        // value 写入的 offset，每个块内单独计算off
        int off;
        try {
            // 如果已存在该key，则在key对应的原off位置写入value
            ByteBuffer buffer = localBufferValue.get();
            buffer.put(value);
            buffer.flip();

            off = map[keyHash].getOrDefault(numkey, -1);

            if (off != -1) {
                //将value写入buffer
                fileChannels[fileHash].write(buffer, (long) off << SHIFT_NUM);
            } else { // 不存在该key时，先判断是否过块，过了则写入temp文件，修改off
                //将value写入buffer
                off = valueOffsets[fileHash].getAndIncrement();
                fileChannels[fileHash].write(buffer, (long) off << SHIFT_NUM);

                // 此时文件中写入的off发生改变
                map[keyHash].put(numkey, off);
                ByteBuffer keyBuffer = localBufferKey.get();
                keyBuffer.putLong(numkey).putInt(off);
                keyBuffer.flip();
                keyFileChannels[keyHash].write(keyBuffer, keyOffsets[keyHash].getAndAdd(KEY_AND_OFF_LEN));
                keyBuffer.clear();
            }
            buffer.clear();
        } catch (
                Exception e)

        {
            e.printStackTrace();
            throw new EngineException(RetCodeEnum.IO_ERROR, "写入数据出错");
        }

    }


    @Override
    public byte[] read(byte[] key) throws EngineException {
        long numkey = Util.bytes2long(key);
        int fileHash = valueFileHash(numkey);
        int off = getKey(numkey);
        if (off == -1) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, numkey + "不存在");
        }
        byte[] bytes = localValueBytes.get();
        try {
            ByteBuffer buffer = localBufferValue.get();
            //如果不在 块中，则去temp文件中读取
            fileChannels[fileHash].read(buffer, (long) off << SHIFT_NUM);
            buffer.flip();
            buffer.get(bytes, 0, VALUE_LEN);
            buffer.clear();
        } catch (Exception e) {
            e.printStackTrace();
            throw new EngineException(RetCodeEnum.IO_ERROR, "read 出错");
        }
        return bytes;
    }


    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        synchronized (threadList) {
            threadList.add(Thread.currentThread());
        }
        int num, count = 0;
        byte[] keyBytes = localKeyBytes.get();
        byte[] valueBytes = localValueBytes.get();
        ByteBuffer buffer;
        logger.info("in range CURRENT_KEY_NUM = " + CURRENT_KEY_NUM);
        try {
            // 第一次初始化sharedBuffer

            for (int i = 0; i < FILE_COUNT; i++) {
                // 64 个屏障都到了才继续运行，运行前先获取buffer
                cyclicBarrier.await(20, TimeUnit.SECONDS);
                //多次执行没关系
                synchronized (cyclicBarrier) {
                    if (cyclicBarrier.isBroken()) {
                        cyclicBarrier.reset();
                    }
                }
                num = valueOffsets[i].get();
                buffer = sharedBuffer.slice();
                logger.info(i + " buffer num: " + num);
                for (int j = 0; j < num; j++) {
                    buffer.position(offs[count + j] << SHIFT_NUM);
                    buffer.get(valueBytes);
                    long2bytes(keyBytes, keys[count + j]);
                    visitor.visit(keyBytes, valueBytes);
                }
                count += num;
                logger.info(i + " read end count: " + count);
//                 只有下一块内存已经准备好之后才继续执行
//                if (fileReadCount < 511) {
//                    LockSupport.parkNanos(20000000000L);
//                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new EngineException(RetCodeEnum.IO_ERROR, "range read io 出错");
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
        logger.info("--------close--------");
    }

    //取前6位，分为64个文件
//    private static int keyFileHash(long key) {
//        return (int) (key >>> 58);
////        return (int) (key & HASH_VALUE);
//    }

    //    取前9位，分为512个文件
    private static int valueFileHash(long key) {
        return (int) (key >>> 55);
//        return (int) (key & HASH_VALUE);
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
            if (j + 1 <= end && (keys[j] < keys[j + 1])) {
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
