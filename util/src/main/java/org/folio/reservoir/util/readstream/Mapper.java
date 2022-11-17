package org.folio.reservoir.util.readstream;

public interface Mapper<T, V> {

  public void push(T item);

  public V poll(boolean ended);
  
}
