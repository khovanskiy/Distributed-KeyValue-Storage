package com.khovanskiy.dkvstorage.vr;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Victor Khovanskiy
 */
public class Looper implements Runnable {

    private LinkedBlockingQueue<Runnable> events = new LinkedBlockingQueue<>();

    @Override
    public void run() {
        while (true) {
            try {
                Runnable event = events.take();
                event.run();
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void run(Runnable event) {
        try {
            events.put(event);
        } catch (InterruptedException ignored) {
        }
    }
}
