package edu.um.civil.pfas.model;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Title implements Serializable {
    // simple version of Task.java
    private String id;
    private String task_name;
}
