package com.groupon.lex.metrics.json;

import com.groupon.lex.metrics.GroupName;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class JsonGroupName {
    private List<String> path;
    private JsonTags tags;

    public JsonGroupName(GroupName group) {
        this(group.getPath().getPath(), JsonTags.valueOf(group.getTags()));
    }
}
