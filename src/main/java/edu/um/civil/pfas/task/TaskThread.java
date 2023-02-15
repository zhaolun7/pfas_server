package edu.um.civil.pfas.task;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TaskThread implements Runnable{
    private String task_id;
    public TaskThread(String task_id) {
        this.task_id = task_id;
    }
    @Override
    public void run() {
        Process proc;
        try {
            String[] args1 = new String[] { "/usr/bin/python3", "python/KMD_MONGO_TASK.py", task_id};
            proc=Runtime.getRuntime().exec(args1);
            //用输入输出流来截取结果
            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = null;
            while ((line = in.readLine()) != null) {
//                System.out.println(line);
            }
            in.close();
            proc.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
