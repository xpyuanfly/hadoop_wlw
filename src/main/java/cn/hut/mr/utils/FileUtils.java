package cn.hut.mr.utils;

import java.io.File;

public class FileUtils {
    public  static  void  delFile(File file)
    {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File f : files) {
                delFile(f);
            }
        }
        file.delete();
    }
}