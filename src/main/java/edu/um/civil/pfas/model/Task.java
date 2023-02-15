package edu.um.civil.pfas.model;


import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

@Data
public class Task implements Serializable {
    private String id;
    private String task_name;
    private String email;
    private List<String> intensity_list;
    private String kmd_width;
    private String mz_width;
    private String precision;
    private String precision_appear_in_all_samples;

    private List<String> files;

    private long update_time; // timestamp

    private HashMap<String, HashMap<String, Object>> status;

    private HashMap<String, Object> result;
}
