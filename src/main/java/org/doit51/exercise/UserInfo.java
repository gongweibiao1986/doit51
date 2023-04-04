package org.doit51.exercise;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class UserInfo {
    private int uid;
    private String gender;
    private String name;
    private List<UserInfo> friends;
}
