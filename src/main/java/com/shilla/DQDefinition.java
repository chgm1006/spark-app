package shilla;

import java.io.Serializable;
import java.util.Map;

public class DQDefinition implements Serializable {

    String definitionId;

    Map<String, String> functionMap;

    public String getDefinitionId() {
        return definitionId;
    }

    public void setDefinitionId(String definitionId) {
        this.definitionId = definitionId;
    }

    public Map<String, String> getFunctionMap() {
        return functionMap;
    }

    public void setFunctionMap(Map<String, String> functionMap) {
        this.functionMap = functionMap;
    }
}
