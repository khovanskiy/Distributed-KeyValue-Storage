package com.khovanskiy.dkvstorage.vr;

import com.khovanskiy.dkvstorage.vr.message.IdentificationMessage;
import com.khovanskiy.dkvstorage.vr.message.Message;
import com.khovanskiy.dkvstorage.vr.operation.DeleteOperation;
import com.khovanskiy.dkvstorage.vr.operation.GetOperation;
import com.khovanskiy.dkvstorage.vr.operation.Operation;
import com.khovanskiy.dkvstorage.vr.operation.SetOperation;

import java.text.ParseException;

/**
 * @author Victor Khovanskiy
 */
public class Utils {
    public static int parseInt(String s, int d) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException ignored) {
            return d;
        }
    }

    public static void log(int i, String s) {
        System.out.println("{REPLICA " + i + "} = " + s);
    }

    public static int parseInt(String s) {
        return parseInt(s, 0);
    }

    /*public static Message parseMessage(String s) throws ParseException {
        String[] slices = s.split(" ");
        switch (slices[0]) {
            case "node": {
                int id = Integer.parseInt(slices[1]);
                return new IdentificationMessage(id);
            }
            case "get": {
                String key = slices[1];
                return new GetOperation(key);
            }
            case "set": {
                String key = slices[1];
                String value = slices[2];
                return new SetOperation(key, value);
            }
            case "delete": {
                String key = slices[1];
                return new DeleteOperation(key);
            }
        }
        throw new ParseException("unknown operation: " + s, 0);
    }*/
}
