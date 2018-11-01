package dataframe;

import java.lang.*;
import java.lang.reflect.InvocationTargetException;

public abstract class Value implements Cloneable,Comparable<Value>{
    public abstract String toString();
    public abstract Value add(Value value);
    public abstract Value sub(Value value);
    public abstract Value mul(Value value);
    public abstract Value div(Value value);
    public abstract Value pow(Value value);
    public abstract boolean eq(Value value);
    public abstract boolean lte(Value value);
    public abstract boolean gte(Value value);
    public abstract boolean neq(Value value);
    public abstract boolean equals(Object other);
    public abstract int hashCode();
    public abstract Value create(String s);
    public abstract Object getValue();


    public abstract int compareTo(Value o);
    public static ValueBuilder builder(Class<? extends Value> c) {
        return new ValueBuilder(c);
    }
    public static class ValueBuilder {
        Class<? extends Value> typ;

        ValueBuilder(Class<? extends Value> c) {
            typ = c;
        }

        public Value build(String data) {
            try {
                return (Value) typ.getMethod("create", String.class).invoke(typ.newInstance(), data);
            } catch (IllegalAccessException | InvocationTargetException | InstantiationException | NoSuchMethodException e) {
                e.printStackTrace();
            }
            throw new RuntimeException();
        }
    }
}
