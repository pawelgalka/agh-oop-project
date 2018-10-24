package dataframe;

import java.util.Objects;

public class ValInteger extends Value{
    private Integer value;
    private static ValInteger integer = new ValInteger();

    public static ValInteger getInstance(){
        return integer;
    }

    private ValInteger(){};

    public ValInteger(final int integer){
        value = integer;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return java.lang.Integer.toString(value);
    }


    @Override
    public Value add(Value value) {
        if (value instanceof ValInteger){
            return new ValInteger(this.value + ((ValInteger) value).getValue());
        }
        return this;
    }

    @Override
    public Value sub(Value value) {
        if (value instanceof ValInteger){
            return new ValInteger(this.value-((ValInteger) value).getValue());
        }
        return this;

    }

    @Override
    public Value mul(Value value) {
        if (value instanceof ValInteger){
            return new ValInteger(this.value*((ValInteger) value).getValue());
        }
        return this;
    }

    @Override
    public Value div(Value value) {
        if (value instanceof ValInteger){
            return new ValInteger(this.value/((ValInteger) value).getValue());
        }
        return this;
    }

    @Override
    public Value pow(Value value) {
        if (value instanceof ValInteger){
            return new ValInteger((int)Math.pow((double)this.value,(double)((ValInteger) value).getValue()));
        }
        return this;
    }

    @Override
    public boolean eq(Value value) {
        if (value instanceof ValInteger){
            return Objects.equals(this.value, ((ValInteger) value).getValue());
        }
        return false;
    }

    @Override
    public boolean lte(Value value) {
        if (value instanceof ValInteger) {
            return this.value <= ((ValInteger) value).getValue();
        }
        return false;
    }

    @Override
    public boolean gte(Value value) {
        if (value instanceof ValInteger) {
            return this.value >= ((ValInteger) value).getValue();
        }
        return false;
    }

    @Override
    public boolean neq(Value value) {
        if (value instanceof ValInteger){
            return !Objects.equals(this.value, ((ValInteger) value).getValue());
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
        value = java.lang.Integer.parseInt(s);
        return new ValInteger(value);
    }

}
