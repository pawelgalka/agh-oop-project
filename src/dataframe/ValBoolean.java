package dataframe;

import java.util.Objects;

public class ValBoolean extends Value{
    private Boolean value;
    private static ValBoolean integer = new ValBoolean();

    public static ValBoolean getInstance(){
        return integer;
    }

    private ValBoolean(){};

    public ValBoolean(final boolean integer){
        value = integer;
    }

    public boolean getValue() {
        return value;
    }

    @Override
    public String toString() {
        return java.lang.Boolean.toString(value);
    }

    public Value add(Value v){
        return this;
    }

    public  Value sub(Value v){
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

    public boolean eq(Value v){
        if (v instanceof ValBoolean){
            return Objects.equals(this.value, ((ValBoolean) v).getValue());
        }
        return false;
    }

    public boolean lte(Value v) {
        if (v instanceof ValBoolean) {
            return true;
        }
        return false;
    }

    public boolean gte(Value v){
        if (v instanceof ValBoolean) {
            return true;
        }
        return false;
    }

    public boolean neq(Value v){
        if (v instanceof ValBoolean) {
            return true;
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
        value = java.lang.Boolean.parseBoolean(s);
        return new ValBoolean(value);
    }
}
