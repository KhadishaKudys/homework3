package enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

@Getter
@AllArgsConstructor
public enum OperationType {
    READ_MULTIPLE_SINGLE_TUPLES_AND_SUM(0.0, 0.6),
    INSERT_A_NEW_ENTRY(0.6, 0.7),
    SUM_WHOLE_TABLE_IN_SQL(0.7, 0.8),
    UPDATE_THREE_TABLE_ENTRIES(0.8, Double.POSITIVE_INFINITY);

    private final double min;
    private final double max;

    public static OperationType fromProbability(final double p) {
        return Arrays.stream(values())
                .filter(operation -> p >= operation.min && p < operation.max)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid probability: " + p));
    }
}




