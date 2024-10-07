package cn.lokn.knmq.store;

import cn.lokn.knmq.model.KNMessage;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import lombok.SneakyThrows;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;

/**
 * @description: mmap store.
 * @author: lokn
 * @date: 2024/09/18 23:10
 */
public class StoreDemo {

    // 文件内存映射
    @SneakyThrows
    public static void main(String[] args) {
        // 两种内存映射的方式，提示操作性能，简化文件的处理方式
        // 1、mmap
        // 2、file.sendFile
        String content = """
                this is a good file.
                that is a new line for store.
                """;
        int length = content.getBytes(StandardCharsets.UTF_8).length;
        System.out.println(" len = " + length);
        File file = new File("text.dat");
        if (!file.exists()) file.createNewFile();

        Path path = Paths.get(file.getAbsolutePath());
        try (FileChannel fileChannel = (FileChannel) Files.newByteChannel(path,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            MappedByteBuffer mappedByteBuffer = fileChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, 1024);
            for (int i = 0; i < 10; i++) {
                System.out.println(i + " -> " + mappedByteBuffer.position());
                KNMessage<?> kn = KNMessage.create(i + ":" + content, null);
                String msg = JSON.toJSONString(kn);
                Indexer.addEntry("test",
                        mappedByteBuffer.position(), msg.getBytes(StandardCharsets.UTF_8).length);
                mappedByteBuffer.put(Charset.forName("UTF-8").encode(msg));
            }

            length += 2;

            ByteBuffer readOnlyBuffer = mappedByteBuffer.asReadOnlyBuffer();
            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {
                String line = sc.nextLine();
                if ("q".equals(line)) return;
                System.out.println(" IN = " + line);
                int id = Integer.parseInt(line);
                Indexer.Entry entry = Indexer.getEntry("test", id);
                readOnlyBuffer.position(entry.getOffset());
                int len = entry.getLength();
                byte[] bytes = new byte[len];
                readOnlyBuffer.get(bytes, 0, len);
                String s = new String(bytes, StandardCharsets.UTF_8);
                System.out.println(" read only => " + s);
                KNMessage<String> message = JSON.parseObject(s, new TypeReference<KNMessage<String>>() {
                });
                System.out.println(" message.body => " + message.getBody());
            }

        }
    }

}
