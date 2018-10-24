package dataframe;

import java.util.Objects;

public class ValDouble extends Value{
    private Double value;
    private static ValDouble integer = new ValDouble();

    public static ValDouble getInstance(){
        return integer;
    }

    private ValDouble(){};

    public ValDouble(final double integer){
        value = integer;
    }

    public double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return java.lang.Double.toString(value);
    }


    @Override
    public Value add(Value value) {
        if (value instanceof ValDouble){
            return new ValDouble(this.value + ((ValDouble) value).getValue());
        }
        return this;
    }

    @Override
    public Value sub(Value value) {
        if (value instanceof ValDouble){
            return new ValDouble(this.value-((ValDouble) value).getValue());
        }
        return this;

    }

    @Override
    public Value mul(Value value) {
        if (value instanceof ValDouble){
            return new ValDouble(this.value*((ValDouble) value).getValue());
        }
        return this;
    }

    @Override
    public Value div(Value value) {
        if (value instanceof ValDouble){
            return new ValDouble(this.value/((ValDouble) value).getValue());
        }
        return this;
    }

    @Override
    public Value pow(Value value) {
        if (value instanceof ValDouble){
            return new ValDouble((int)Math.pow((double)this.value,(double)((ValDouble) value).getValue()));
        }
        return this;
    }

    @Override
    public boolean eq(Value value) {
        if (value instanceof ValDouble){
            return Objects.equals(this.value, ((ValDouble) value).getValue());
        }
        return false;
    }

    @Override
    public boolean lte(Value value) {
        if (value instanceof ValDouble) {
            return this.value <= ((ValDouble) value).getValue();
        }
        return false;
    }

    @Override
    public boolean gte(Value value) {
        if (value instanceof ValDouble) {
            return this.value >= ((ValDouble) value).getValue();
        }
        return false;
    }

    @Override
    public boolean neq(Value value) {
        if (value instanceof ValDouble){
            return !Objects.equals(this.value, ((ValDouble) value).getValue());
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
        value = java.lang.Double.parseDouble(s);
        return new ValDouble(value);
    }

}
