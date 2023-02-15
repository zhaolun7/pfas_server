package edu.um.civil.pfas.controller;

import edu.um.civil.pfas.model.Task;
import edu.um.civil.pfas.model.Title;
import edu.um.civil.pfas.repository.TaskRepository;
import edu.um.civil.pfas.task.TaskThread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.UUID;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@CrossOrigin
@RestController
public class TaskController {
    @Autowired
    private TaskRepository taskRepository;
    private ExecutorService executorService = Executors.newFixedThreadPool(3);

    @RequestMapping( value = "/submit", method = RequestMethod.POST)
    @ResponseBody
    public Object submitNewTask(@RequestBody Task t) {
        String task_id = taskRepository.insertTask(t);
        HashMap<String, Object> result = new HashMap<>();
        if(task_id != null) {
            result.put("code", 0);
            result.put("msg", "succ");
            result.put("task_id", task_id);
            executorService.submit(new TaskThread(task_id));
        } else {
            result.put("code", -1);
            result.put("msg", "error when submitting new task");
        }
        return result;
    }

    @RequestMapping( value = "/track_last_n_task_names", method = RequestMethod.GET)
    @ResponseBody
    public Object getLastTasks(@RequestParam(value = "num", required = false, defaultValue = "1") Integer n) {
        HashMap<String, Object> result = new HashMap<>();
        // TODO prevent from attacking
        if(n > 100) n = 100;
        List<Title> tasks = taskRepository.getLastNTasks(n);
        if(tasks != null) {
            result.put("code", 0);
            result.put("msg", "succ");
            result.put("tasks", tasks);
            result.put("num", tasks.size());
        } else {
            result.put("code", 0);
            result.put("msg", "error when getting last tasks.");
        }
        return result;
    }
    @RequestMapping( value = "/track_file_info", method = RequestMethod.GET)
    @ResponseBody
    public Object getFileInfoById(@RequestParam(value = "task_id", required = false, defaultValue = "-") String id) {
        HashMap<String, Object> result = null;
        HashMap task = taskRepository.findFileInfoByTaskId(id);
        if(task != null) {
            result = new HashMap<>();
            result.put("code", 0);
            result.put("msg", "succ");
            result.put("task", task);
        }
        return result;
    }


    @PostMapping("/upload")
    @ResponseBody
    public Object singleFileUpload(@RequestParam("file") MultipartFile file) throws Exception{
        HashMap<String, Object> result = new HashMap<>();
        if (file.isEmpty()) {
            result.put("code", -1);
            result.put("msg", "empty file");
            return result;
        }
        try {
            // Get the file and save it somewhere
            byte[] bytes = file.getBytes();
            byte[] array = new byte[5]; // length is bounded by 7
            new Random().nextBytes(array);
            String randomPath = System.currentTimeMillis()/1000 + "_" + UUID.randomUUID().toString().substring(1,8);
            String path = "data-input/" + randomPath;
            System.out.println("random path:"+path);
            File folder = new File(path);
            if(!folder.mkdir()){
                throw new IOException("error, can't create folder!");
            }
            Path filePath = Paths.get(path + "/" + file.getOriginalFilename());
            Files.write(filePath, bytes);
            result.put("code", 0);
            result.put("msg", "succ");
            result.put("file", randomPath+"/"+file.getOriginalFilename());
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }

        return result;
    }

    @RequestMapping("/result_file_download")
    public void baseCacheDownload(@RequestParam("file") String filepath, HttpServletResponse response) throws Exception {
        FileSystemResource file = new FileSystemResource("data-output/"+filepath);
        System.out.println("filepath:"+filepath);
        if (!file.exists()) {
            throw new Exception("file not exists");
        }
//        System.out.println("receive request!!");
        InputStream inputStream = file.getInputStream();
        response.reset();
        response.setContentType("application/octet-stream");
        String filename = file.getFilename();
        response.addHeader("Content-Disposition", "attachment; filename=" + URLEncoder.encode(filename, "UTF-8"));//①tips
        ServletOutputStream outputStream = response.getOutputStream();
        byte[] b = new byte[1024];
        int len;
        //缓冲区（自己可以设置大小）：从输入流中读取一定数量的字节，并将其存储在缓冲区字节数组中，读到末尾返回-1
        while ((len = inputStream.read(b)) >= 0) {
            outputStream.write(b, 0, len);
        }
        inputStream.close();
    }

}
