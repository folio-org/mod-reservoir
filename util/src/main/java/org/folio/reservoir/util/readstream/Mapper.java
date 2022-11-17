package org.folio.reservoir.util.readstream;

/**
 * Represents a FIFO queue for mapping elements from one type to another.
 * Implementations of this classed are to be used with the MappingReadStream
 */
public interface Mapper<T, V> {

  /**
   * Store the element for next mapping.
   * This is a blocking operation so the implementation should ensure that it returns quickly.
   * @param item element to be mapped
   */
  public void push(T item);

  /**
   * Retreive the last mapped element. If the element is not ready, 'null' is returned.
   * Note that the poll(true) can be called multiple times
   * @param ended true indicates that there will be no more push calls.
   * @return mapped element
   */
  public V poll(boolean ended);
  
}
