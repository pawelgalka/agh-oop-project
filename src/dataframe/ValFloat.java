package dataframe;

import java.util.Objects;

public class ValFloat extends Value{
    private Float value;
    private static ValFloat integer = new ValFloat();

    public static ValFloat getInstance(){
        return integer;
    }

    private ValFloat(){};

    public ValFloat(final float integer){
        value = integer;
    }

    public float getValue() {
        return value;
    }

    @Override
    public String toString() {
        return java.lang.Float.toString(value);
    }


    @Override
    public Value add(Value value) {
        if (value instanceof ValFloat){
            return new ValFloat(this.value + ((ValFloat) value).getValue());
        }
        return this;
    }

    @Override
    public Value sub(Value value) {
        if (value instanceof ValFloat){
            return new ValFloat(this.value-((ValFloat) value).getValue());
        }
        return this;

    }

    @Override
    public Value mul(Value value) {
        if (value instanceof ValFloat){
            return new ValFloat(this.value*((ValFloat) value).getValue());
        }
        return this;
    }

    @Override
    public Value div(Value value) {
        if (value instanceof ValFloat){
            return new ValFloat(this.value/((ValFloat) value).getValue());
        }
        return this;
    }

    @Override
    public Value pow(Value value) {
        if (value instanceof ValFloat){
            return new ValFloat((int)Math.pow((double)this.value,(double)((ValFloat) value).getValue()));
        }
        return this;
    }

    @Override
    public boolean eq(Value value) {
        if (value instanceof ValFloat){
            return Objects.equals(this.value, ((ValFloat) value).getValue());
        }
        return false;
    }

    @Override
    public boolean lte(Value value) {
        if (value instanceof ValFloat) {
            return this.value <= ((ValFloat) value).getValue();
        }
        return false;
    }

    @Override
    public boolean gte(Value value) {
        if (value instanceof ValFloat) {
            return this.value >= ((ValFloat) value).getValue();
        }
        return false;
    }

    @Override
    public boolean neq(Value value) {
        if (value instanceof ValFloat){
            return !Objects.equals(this.value, ((ValFloat) value).getValue());
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
        value = java.lang.Float.parseFloat(s);
        return new ValFloat(value);
    }
}
