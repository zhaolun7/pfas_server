package edu.um.civil.pfas.repository;

import edu.um.civil.pfas.model.Task;
import edu.um.civil.pfas.model.Title;
import org.springframework.data.mongodb.repository.Query;

import java.util.HashMap;
import java.util.List;

public interface TaskRepository {

    public String insertTask(Task task);

    public List<Title> getLastNTasks(int n);
    public HashMap findFileInfoByTaskId(String task_id);

    public long updateTask(Task task);

    public void deleteTaskById(String id);
}
