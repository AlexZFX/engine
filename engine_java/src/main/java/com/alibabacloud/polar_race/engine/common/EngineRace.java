package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class EngineRace extends AbstractEngine {
    // key 长度 8B
    private static final int KEY_LEN = 8;
    // value 长度 4K
    private static final int VALUE_LEN = 4096;
    //    单个线程写入消息 100w
    private static final int MSG_COUNT = 1000000;
    //    64 个线程
    private static final int THREAD_COUNT = 64;
    //    64个线程写消息 6400w
    private static final int ALL_MSG_COUNT = 64000000;
    //    每个文件存放 400w 个数据
    private static final int MSG_COUNT_PERFILE = 4000000;
    //    存放 value+key 的文件大小
    private static final int FILE_LEN = (KEY_LEN + VALUE_LEN) * MSG_COUNT_PERFILE;
    //    存放 value 的文件数量 16
    private static final int FILE_COUNT = 16;

    private static final int INDEX_LEN = ALL_MSG_COUNT * 4;

    private ByteBuffer indexBuffer;
    private ByteBuffer dataBuffer = ByteBuffer.allocate(KEY_LEN + VALUE_LEN);
    // data 编号，总共 6400w 个
    private AtomicInteger dataNo = new AtomicInteger(1);
    private ArrayList<RandomAccessFile> dataFiles = new ArrayList<>();
    private ArrayList<FileChannel> dataFileChannels = new ArrayList<>();


    @Override
    public void open(String path) throws EngineException, IOException {
        indexBuffer = new RandomAccessFile("index", "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, INDEX_LEN >> 2 + 20);
        for (int i = 0; i < FILE_COUNT; i++) {
            RandomAccessFile file = new RandomAccessFile("data" + i, "rw");
            dataFileChannels.add(file.getChannel());
            dataFiles.add(file);
        }
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        int hash = key.hashCode() % INDEX_LEN * 4;
        dataBuffer.put(key);
        dataBuffer.put(value);
        dataBuffer.flip();
        try {
            dataFileChannels.get(dataNo.get() / MSG_COUNT_PERFILE).write(dataBuffer,dataNo.get()*(KEY_LEN+VALUE_LEN));
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            indexBuffer.position(hash);
            int index = indexBuffer.getInt();
            if (index == 0) {
                indexBuffer.position(hash);
                indexBuffer.putInt(dataNo.getAndIncrement());
                break;
            } else {
                byte[] sameKey = new byte[8];
                getKey(sameKey, dataNo.get());
                if (Arrays.equals(key, sameKey)) {
                    indexBuffer.position(hash + 8);
                    indexBuffer.put(value);
                    break;
                } else {
                    hash = (hash + 4) % INDEX_LEN * 4;
                }
            }
        }
    }



    @Override
    public byte[] read(byte[] key) throws EngineException {
        byte[] value = null;
        int hash = key.hashCode() % INDEX_LEN * 4;
        while (true) {
            indexBuffer.position(hash);
            int position = indexBuffer.getInt();
            if (position == 0) {
                throw new EngineException(RetCodeEnum.NOT_FOUND, "Not Found");
            }
            byte[] sameKey = new byte[8];
            getKey(sameKey, position);
            if (Arrays.equals(key, sameKey)) {
                getValue(value, position);
                return value;
            } else {
                hash = (hash + 4) % INDEX_LEN * 4;
            }

        }
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
    }

    @Override
    public void close() {
        for (int i = 0; i < FILE_COUNT; i++) {
            try {
                RandomAccessFile file = new RandomAccessFile("data" + i, "rw");
                dataFileChannels.get(i).close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void getKey(byte[] bytes, int position) {
        int fileIndex = position / MSG_COUNT_PERFILE;
        try {
            dataFileChannels.get(fileIndex).read(dataBuffer,(KEY_LEN + VALUE_LEN) % FILE_LEN);
            dataBuffer.get(bytes,0,KEY_LEN);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getValue(byte[] bytes, int position) {
        int fileIndex = position / MSG_COUNT_PERFILE;
        try {
            dataFileChannels.get(fileIndex).read(dataBuffer,(KEY_LEN + VALUE_LEN) % FILE_LEN + 8);
            dataBuffer.get(bytes,0,VALUE_LEN);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
