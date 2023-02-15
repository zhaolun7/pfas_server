package edu.um.civil.pfas.repository.impl;

import com.mongodb.client.result.UpdateResult;
import edu.um.civil.pfas.model.Task;
import edu.um.civil.pfas.model.Title;
import edu.um.civil.pfas.repository.TaskRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Component
public class TaskRepositoryImpl implements TaskRepository {
    @Autowired
    private MongoTemplate mongoTemplate;



    private long getUpdateTime() {
        return System.currentTimeMillis()/1000;
    }
    @Override
    public String insertTask(Task task) {
        HashMap<String, HashMap<String, Object>> status = new HashMap<>();
        task.setUpdate_time(getUpdateTime());
        task.setStatus(status);

        List<String> files = task.getFiles();
        if(files !=null && !files.isEmpty()) {
            for(String file: files) {
                if("database.xlsx".equals(file) || "database.xls".equals(file)) {
                    continue;
                }
                HashMap<String, Object> sub_file_status = new HashMap<>();
                status.put(file.replace('.','_'), sub_file_status);
                sub_file_status.put("name", file.split("/")[1]);
                sub_file_status.put("step", 0);
                sub_file_status.put("status", 0);
            }

        }


        Task t = mongoTemplate.save(task);
        if(t != null) {
            return t.getId();
        }
        return null;
    }

    @Override
    public List<Title> getLastNTasks(int n) {
        ArrayList<Task> arr_result = new ArrayList<>();
        Query query = new Query();
        query.fields().include("task_name");
        query.limit(n);
        query.with(Sort.by(Sort.Direction.DESC,"update_time"));
        List<Title> result =mongoTemplate.find(query, Title.class, "task");
        return result;
    }

    @Override
    public HashMap findFileInfoByTaskId(String task_id) {
        Query query=new Query(Criteria.where("id").is(task_id));
        Task task =  mongoTemplate.findOne(query , Task.class);
        HashMap<String, Object> result = null;
        if(task != null) {
            ArrayList<HashMap<String, Object>> details = new ArrayList<>(task.getFiles().size());
            for(String file:task.getFiles()) {
                if("database.xlsx".equals(file) || "database.xls".equals(file)) {
                    continue;
                }
                details.add(task.getStatus().get(file.replace('.', '_')));
            }
            result = new HashMap<>();
            result.put("taskID", task.getId());
            result.put("taskName", task.getTask_name());
            result.put("details", details);
            result.put("result", task.getResult());
        }
        return result;
    }

    @Override
    public long updateTask(Task task) {
//        Query query=new Query(Criteria.where("id").is(task.getId()));
//        Update update= new Update()
//                .set("task_name", task.getTask_name())
//                .set("email", task.getEmail())
//                .set("intensity_list", task.getIntensity_list())
//                .set("kmd_width", task.getKmd_width())
//                .set("mz_width", task.getMz_width())
//                .set("precision", task.getPrecision())
//                .set("precision_appear_in_all_samples", task.getPrecision_appear_in_all_samples());
//        UpdateResult result =mongoTemplate.updateFirst(query,update,Task.class);
//        if(result!=null)
//            return result.getMatchedCount();
//        else
            return 0;
    }

    @Override
    public void deleteTaskById(String id) {
//        Query query=new Query(Criteria.where("id").is(id));
//        mongoTemplate.remove(query,Task.class);
    }
}
