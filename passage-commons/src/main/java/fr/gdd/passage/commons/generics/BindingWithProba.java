package fr.gdd.passage.commons.generics;

public class BindingWithProba<ID, VALUE> {
    private BackendBindings<ID, VALUE> bindings;
    private double proba;

    public BindingWithProba(BackendBindings<ID, VALUE> bindings, double proba) {
        this.bindings = bindings;
        this.proba = proba;
    }

    public BackendBindings<ID, VALUE> getBindings() {
        return bindings;
    }

    public double getProba() {
        return proba;
    }
}