package org.doit51.java;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

@Data
@NoArgsConstructor
@ToString
@AllArgsConstructor
public class EventLog {
    private long guid;
    private String sessionId;
    private String eventId;
    private long timeStamp;
    private Map<String, String> eventInfo;
}
