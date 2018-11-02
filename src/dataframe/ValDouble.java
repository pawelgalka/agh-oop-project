package dataframe;

import javax.xml.validation.Validator;
import java.util.Objects;

public class ValDouble extends Value{
    private Double value;
    /*private static ValDouble integer = new ValDouble();

    public static ValDouble getInstance(){
        return integer;
    }*/

    ValDouble(){};

    public ValDouble(final double integer){
        value = integer;
    }

    public Double getValue() {
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
        else if (value instanceof ValInteger){
            return new ValDouble(this.value+((ValInteger) value).getValue());
        }
        else if (value instanceof ValFloat){
            return new ValDouble(this.value+((ValFloat) value).getValue());
        }
        else System.out.println("Tried invalid operation +");
        return this;
    }

    @Override
    public Value sub(Value value) {
        if (value instanceof ValDouble){
            return new ValDouble(this.value-((ValDouble) value).getValue());
        }
        else if (value instanceof ValInteger){
            return new ValDouble(this.value-((ValInteger) value).getValue());
        }
        else if (value instanceof ValFloat){
            return new ValDouble(this.value-((ValFloat) value).getValue());
        }
        else System.out.println("Tried invalid operation -");
        return this;

    }

    @Override
    public Value mul(Value value) {
        if (value instanceof ValDouble){
            return new ValDouble(this.value*((ValDouble) value).getValue());
        }
        else if (value instanceof ValInteger){
            return new ValDouble(this.value*((ValInteger) value).getValue());
        }
        else if (value instanceof ValFloat){
            return new ValDouble(this.value*((ValFloat) value).getValue());
        }
        else System.out.println("Tried invalid operation *");
        return this;
    }

    @Override
    public Value div(Value value) {
        if (value instanceof ValDouble){
            return new ValDouble(this.value/((ValDouble) value).getValue());
        }
        else if (value instanceof ValInteger){
            return new ValDouble(this.value/((ValInteger) value).getValue());
        }
        else if (value instanceof ValFloat){
            return new ValDouble(this.value/((ValFloat) value).getValue());
        }
        else System.out.println("Tried invalid operation /");

        return this;
    }

    @Override
    public Value pow(Value value) {
        if (value instanceof ValDouble){
            return new ValDouble(Math.pow((double)this.value,(double)((ValDouble) value).getValue()));
        }
        else if (value instanceof  ValInteger){
            return new ValDouble(Math.pow((double)this.value,(double)((ValInteger) value).getValue()));
        }
        else if (value instanceof  ValFloat){
            return new ValDouble(Math.pow((double)this.value,(double)((ValFloat) value).getValue()));
        }
        else System.out.println("Tried invalid operation ^");
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValDouble valDouble = (ValDouble) o;
        return Objects.equals(value, valDouble.value);
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
    @Override
    public int compareTo(Value o) {
        return value.compareTo(((ValDouble)o).getValue());
    }

}
