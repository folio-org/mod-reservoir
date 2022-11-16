package org.folio.reservoir.util.readstream;

public interface Mapper<T, V> {

  public void put(T item);

  public V get(boolean ended);
  
}
