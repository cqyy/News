package com.cqyuanye.utils;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by yuanye on 2016/5/10.
 */
public class MD5 {

    private MD5(){}


    public static String toMd5(String str){
        String result = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] bytesOfStr = str.getBytes("UTF-8");

            byte[] digest = md5.digest(bytesOfStr);

            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest){
                sb.append(Integer.toHexString( (b & 0xff)|0x100).substring(1,3));
            }
            result = sb.toString();
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
           //wouldn't hadppen.
        }
        return result;
    }
}
