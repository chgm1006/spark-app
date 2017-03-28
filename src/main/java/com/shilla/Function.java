package shilla;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

public class Function implements Serializable {

    boolean evaluate(Object value) {
        // MVEL로 evaluate
        return !StringUtils.isEmpty(value + "");
    }
}
