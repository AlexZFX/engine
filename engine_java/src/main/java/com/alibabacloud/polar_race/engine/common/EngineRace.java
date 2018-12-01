package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;
import com.carrotsearch.hppc.LongIntHashMap;
import io.netty.util.concurrent.FastThreadLocal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
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

    private static final int SHIFT_NUM = 12;
    // 存放 value 的文件数量 128
    private static final int FILE_COUNT = 256;

    //128块 1块 8.4375m = 8640 KB = 8847360 B  1个文件 1080m
    private static final int VALUE_FILE_SIZE = 1132462080;

    private static final int BLOCK_NUM = 128;

    private static final int BLOCK_SIZE = 8847360;
    // BLOCK_SIZE / VALUE_LEN
    private static final int MAX_NUM_PER_BLOCK = 2160;

    private static final int HASH_VALUE = 0x3F;
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

    private static MappedByteBuffer[] valueMappedByteBuffers = new MappedByteBuffer[FILE_COUNT];

    //value 文件的fileChannel
    private static FileChannel[] fileChannels = new FileChannel[FILE_COUNT];
    //每个valueOffsets表示的都是第i个文件中的value数量
    private static AtomicInteger[][] valueOffsets = new AtomicInteger[FILE_COUNT][BLOCK_NUM];
    //temp off 直接从 max_num_per_block开始计算，避免和正常的offset弄混
    private AtomicInteger tempOffset = new AtomicInteger(MAX_NUM_PER_BLOCK);

    private FileChannel tempValueFileChannel;

    private static FastThreadLocal<ByteBuffer> localBufferKey = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocate(KEY_AND_OFF_LEN);
        }
    };

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

    private static FastThreadLocal<byte[]> localValueBytes = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() throws Exception {
            return new byte[VALUE_LEN];
        }
    };

    private static FastThreadLocal<ByteBuffer> localBlockBuffer = new FastThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() throws Exception {
            return ByteBuffer.allocateDirect(BLOCK_SIZE);
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
                for (int i = 0; i < FILE_COUNT; i++) {
                    try {
                        randomAccessFile = new RandomAccessFile(path + File.separator + i + ".data", "rw");
                        FileChannel channel = randomAccessFile.getChannel();
                        fileChannels[i] = channel;
                        // 从 length处直接写入
                        valueMappedByteBuffers[i] = channel.map(FileChannel.MapMode.READ_WRITE, 0, VALUE_FILE_SIZE);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                randomAccessFile = new RandomAccessFile(path + File.separator + "temp.data", "rw");
                tempValueFileChannel = randomAccessFile.getChannel();
                for (int i = 0; i < FILE_COUNT; i++) {
                    for (int j = 0; j < BLOCK_NUM; j++) {
                        valueOffsets[i][j] = new AtomicInteger(0);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            throw new EngineException(RetCodeEnum.IO_ERROR, "path不是一个目录");
        }
    }

//    private int handleDuplicate(int keyNum) {
//        int maxNum = 1;
//        for (int i = 1; i < keyNum; ++i) {
//            if (keys[i] != keys[i - 1]) {
//                keys[maxNum] = keys[i];
//                offs[maxNum] = offs[i];
//                maxNum++;
//            }
//        }
//        return maxNum;
//    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        long numkey = Util.bytes2long(key);
        int keyHash = keyFileHash(numkey);
        int fileHash = valueFileHash(numkey);
        int blockHash = valueBlockHash(numkey);
        // value 写入的 offset，每个块内单独计算off
        int off;
        try {
            // 如果已存在该key，则在key对应的原off位置写入value
            if (map[keyHash].containsKey(numkey)) {
                off = map[keyHash].get(numkey);
                if (off >= MAX_NUM_PER_BLOCK) {
                    ByteBuffer buffer = localBufferValue.get();
                    buffer.put(value);
                    buffer.flip();
                    tempValueFileChannel.write(buffer, ((long) off) << SHIFT_NUM);
                    buffer.clear();
                } else {
                    //将value写入buffer
                    ByteBuffer valueBuffer = valueMappedByteBuffers[fileHash].slice();
                    valueBuffer.position((blockHash * BLOCK_SIZE) + (off << SHIFT_NUM));
                    valueBuffer.put(value);
                }
            } else { // 不存在该key时，先判断是否过块，过了则写入temp文件，修改off
                off = valueOffsets[fileHash][blockHash].getAndIncrement();
                if (off >= MAX_NUM_PER_BLOCK) {
                    ByteBuffer buffer = localBufferValue.get();
                    buffer.put(value);
                    buffer.flip();
                    //因为off过块了，写入到temp文件中去
                    off = tempOffset.getAndIncrement();
                    tempValueFileChannel.write(buffer, ((long) off) << SHIFT_NUM);
                    buffer.clear();
                } else {
                    //将value写入buffer
                    ByteBuffer valueBuffer = valueMappedByteBuffers[fileHash].slice();
                    valueBuffer.position((blockHash * BLOCK_SIZE) + (off << SHIFT_NUM));
                    valueBuffer.put(value);
                }
                // 此时文件中写入的off发生改变
                map[keyHash].put(numkey, off);
                ByteBuffer keyBuffer = localBufferKey.get();
                keyBuffer.putLong(numkey).putInt(off);
                keyBuffer.flip();
                keyFileChannels[keyHash].write(keyBuffer, keyOffsets[keyHash].getAndAdd(KEY_AND_OFF_LEN));
                keyBuffer.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new EngineException(RetCodeEnum.IO_ERROR, "写入数据出错");
        }
    }


    @Override
    public byte[] read(byte[] key) throws EngineException {
        long numkey = Util.bytes2long(key);
        int fileHash = valueFileHash(numkey), blockHash = valueBlockHash(numkey);
        int off = getKey(numkey);
        if (off == -1) {
            throw new EngineException(RetCodeEnum.NOT_FOUND, numkey + "不存在");
        }
        byte[] bytes = localValueBytes.get();
        try {
            //如果不在 块中，则去temp文件中读取
            if (off >= MAX_NUM_PER_BLOCK) {
                ByteBuffer buffer = localBufferValue.get();
                tempValueFileChannel.read(buffer, ((long) off) << SHIFT_NUM);
                buffer.flip();
                buffer.get(bytes, 0, VALUE_LEN);
                buffer.clear();
            } else {
                ByteBuffer buffer = valueMappedByteBuffers[fileHash].slice();
                buffer.position((blockHash * BLOCK_SIZE) + (off << SHIFT_NUM));
                buffer.get(bytes, 0, VALUE_LEN);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new EngineException(RetCodeEnum.IO_ERROR, "read 出错");
        }
        return bytes;
    }


    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
        long key;
        int fileHash, blockHash, off;
        byte[] keyBytes = localKeyBytes.get();
        byte[] valueBytes = localValueBytes.get();
        ByteBuffer valueBuffer = localBufferValue.get();
        ByteBuffer blockBuffer = localBlockBuffer.get();
        logger.info("in range CURRENT_KEY_NUM = " + CURRENT_KEY_NUM);
        try {
            for (int i = 0; i < CURRENT_KEY_NUM; ) {
                key = keys[i];
                fileHash = valueFileHash(key);
                blockHash = valueBlockHash(key);
                fileChannels[fileHash].read(blockBuffer, blockHash * BLOCK_SIZE);
                while (fileHash == valueFileHash(keys[i]) && blockHash == valueBlockHash(keys[i])) {
                    off = offs[i];
                    //如果 off 大于块内最多，说明在temp文件中
                    if (off >= MAX_NUM_PER_BLOCK) {
                        tempValueFileChannel.read(valueBuffer, ((long) off) << SHIFT_NUM);
                        valueBuffer.flip();
                        valueBuffer.get(valueBytes);
                        valueBuffer.clear();
                    } else {
                        blockBuffer.position(off << SHIFT_NUM);
                        blockBuffer.get(valueBytes);
                    }
                    long2bytes(keyBytes, keys[i]);
                    visitor.visit(keyBytes, valueBytes);
                    ++i;
                }
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
        for (int i = 0; i < FILE_COUNT; i++) {
            try {
//                logger.info("file" + i + " size is " + valueOffsets[i].get());
                keyFileChannels[i].close();
                fileChannels[i].close();
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("close error");
            }
        }
    }

    //取前6位，分为64个文件
    private static int keyFileHash(long key) {
        return (int) (key >>> 58);
//        return (int) (key & HASH_VALUE);
    }

    //取前8位，分为256个文件
    private static int valueFileHash(long key) {
        return (int) (key >>> 56);
//        return (int) (key & HASH_VALUE);
    }

    // value文件分128个block
    private static int valueBlockHash(long key) {
        return (int) ((key >>> 49) & 0x7F);
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
