package shilla;

import java.io.Serializable;

public class Column implements Serializable {

    String name;
    String type;
    String dqType;
    String dqFunction;
    boolean nullable;

    public Column(String name, String type, boolean nullable) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
    }

    public Column(String name, String type, boolean nullable, String dqType, String dqFunction) {
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.dqType = dqType;
        this.dqFunction = dqFunction;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getDqType() {
        return dqType;
    }

    public void setDqType(String dqType) {
        this.dqType = dqType;
    }

    public String getDqFunction() {
        return dqFunction;
    }

    public void setDqFunction(String dqFunction) {
        this.dqFunction = dqFunction;
    }
}
