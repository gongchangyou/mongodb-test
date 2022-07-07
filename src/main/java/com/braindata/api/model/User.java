package com.braindata.api.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * @author gongchangyou
 * @version 1.0
 * @date 2022/3/15 12:27 下午
 */
@Data
@Builder
public class User {
    private Long id;
    private int age;
    private String name;
    private String country;
    private String category;
    private List<String> nickname;

}
