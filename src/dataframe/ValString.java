package dataframe;

import java.util.Objects;

public class ValString extends Value{
    private String value;
    private static ValString integer = new ValString();

    public static ValString getInstance(){
        return integer;
    }

    private ValString(){};

    public ValString(final String integer){
        value = integer;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }


    @Override
    public Value add(Value value) {
        if (value instanceof ValString){
            return new ValString(this.value + ((ValString) value).getValue());
        }
        return this;
    }

    @Override
    public Value sub(Value value) {
        return this;
    }

    @Override
    public Value mul(Value value) {
        return this;
    }

    @Override
    public Value div(Value value) {
        return this;
    }

    @Override
    public Value pow(Value value) {
        return this;
    }

    @Override
    public boolean eq(Value value) {
        if (value instanceof ValString){
            return Objects.equals(this.value, ((ValString) value).getValue());
        }
        return false;
    }

    @Override
    public boolean lte(Value value) {
        if (value instanceof ValString) {
            return this.value.length() <= ((ValString) value).getValue().length();
        }
        return false;
    }

    @Override
    public boolean gte(Value value) {
        if (value instanceof ValString) {
            return this.value.length() >= ((ValString) value).getValue().length();
        }
        return false;
    }

    @Override
    public boolean neq(Value value) {
        if (value instanceof ValString){
            return !Objects.equals(this.value, ((ValString) value).getValue());
        }
        return false;
    }

    @Override
    public boolean equals(Object other) {
        return value.equals(other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public Value create(String s) {
        value = s;
        return new ValString(value);
    }
}
