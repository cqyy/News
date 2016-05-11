package com.cqyuanye.index;

import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;

/**
 * Created by yuanye on 2016/5/6.
 */
public class NewsFileVisitor extends SimpleFileVisitor<Path> {

//    @Override
//    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
//        File file = path.toFile();
//        if (file.toString().endsWith(".json")){
//            byte[] content = new byte[(int) file.length()];
//            FileInputStream inputStream = new FileInputStream(file);
//            inputStream.read(content);
//            Index2ES.getInstance().index(new String(content,"utf-8"));
//        }
//        return FileVisitResult.CONTINUE;
//    }
//
//    public static void main(String[] args) throws IOException {
//        Files.walkFileTree(Paths.get("E:\\data\\crawl\\news\\人民网"),new NewsFileVisitor());
//    }
}
