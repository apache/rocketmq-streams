/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.streams.state;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * lru && o(1)
 *
 * @author arthur.liang
 */
public final class LruState<T> {

    private int max_size;

    private final T FLAG_VALUE;

    private ConcurrentHashMap<T, Element<T>> elementMap;

    private LinkedList<T> elementList;

    public LruState(int the_size, T flag) {
        max_size = the_size;
        FLAG_VALUE = flag;
        elementMap = new ConcurrentHashMap<>(max_size);
        elementList = new LinkedList<>(FLAG_VALUE);
    }

    /**
     * add
     *
     * @param value
     * @return
     */
    public synchronized boolean add(T value) {
        if (FLAG_VALUE.equals(value)) {
            return false;
        }
        Element<T> newElement = new Element<T>(value);
        if (elementMap.containsKey(value)) {
            Element<T> oldElement = elementMap.get(value);
            elementList.median = oldElement.next;
            elementList.removeElement(oldElement);
            newElement.increase(oldElement.getCounter());
        } else if (elementMap.size() >= max_size) {
            Element<T> deletedElement = elementList.removeElement();
            elementMap.remove(deletedElement.getValue());
        }
        elementList.addElement(newElement);
        elementMap.put(value, newElement);
        return true;
    }

    /**
     * remove
     *
     * @param value
     * @return
     */
    public synchronized boolean remove(T value) {
        if (!elementMap.containsKey(value)) {
            return false;
        }
        Element<T> theElement = elementMap.get(value);
        elementList.removeElement(theElement);
        elementMap.remove(value);
        return true;
    }

    /**
     * search
     *
     * @param value
     * @return if contains return the hit counts else return 0
     */
    public synchronized int search(T value) {
        if (elementMap.containsKey(value)) {
            return elementMap.get(value).getCounter();
        } else {
            return 0;
        }
    }

    /**
     * iterator
     *
     * @return
     */
    public synchronized List<T> getAll() {
        ArrayList<T> iteratorList = new ArrayList<T>();
        Element<T> pointElement = elementList.head.next;
        while (pointElement.next != null) {
            iteratorList.add(pointElement.getValue());
            pointElement = pointElement.next;
        }
        return iteratorList;
    }

    public synchronized Iterator<T> iterator() {
        return new LruIterator();
    }

    public class LruIterator implements Iterator<T> {

        private Element<T> current = elementList.head.next;

        @Override public boolean hasNext() {
            return current != null && !current.equals(elementList.tail);
        }

        @Override public T next() {
            T value = current.getValue();
            current = current.next;
            return value;
        }
    }

    /**
     * counter
     *
     * @return
     */
    public synchronized int count() {
        return elementMap.size();
    }

}

class Element<T> {

    private T value;

    private volatile int counter;

    Element<T> next;

    Element<T> pre;

    public Element(T element) {
        this.value = element;
        counter = 1;
    }

    public T getValue() {
        return this.value;
    }

    public void increase(int number) {
        counter += number;
    }

    public int getCounter() {
        return counter;
    }

}

class LinkedList<T> {

    volatile int current_size = 0;

    Element<T> head;

    Element<T> tail;

    Element<T> median;

    public LinkedList(T flag) {
        head = new Element<T>(flag);
        tail = new Element<T>(flag);
        head.pre = null;
        tail.next = null;
    }

    /**
     * add one element
     *
     * @param theOne
     * @return isSuccess
     */
    public boolean addElement(Element<T> theOne) {
        if (theOne == null) {
            return false;
        }
        if (current_size == 0) {
            head.next = theOne;
            theOne.pre = head;
            theOne.next = tail;
            tail.pre = theOne;
        } else {
            Element<T> current;
            if (median != null && theOne.getCounter() > median.getCounter()) {
                current = median.pre;
            } else {
                current = tail.pre;
            }
            while (current != null && current != head) {
                if (theOne.getCounter() <= current.getCounter()) {
                    break;
                } else {
                    current = current.pre;
                }
            }
            current.next.pre = theOne;
            theOne.next = current.next;
            current.next = theOne;
            theOne.pre = current;
        }
        current_size++;
        median = theOne;
        return true;
    }

    /**
     * remove the element at the end of list
     *
     * @return isSuccess
     */
    public Element<T> removeElement() {
        Element<T> theElement = tail.pre;
        if (theElement == null || theElement == head) {
            return null;
        }
        removeElement(theElement);
        return theElement;
    }

    /**
     * remove the special element
     *
     * @param oneElement
     * @return
     */
    public boolean removeElement(Element<T> oneElement) {
        if (current_size == 0) {
            return false;
        }
        oneElement.next.pre = oneElement.pre;
        oneElement.pre.next = oneElement.next;
        if (median != null && median.equals(oneElement)) {
            median = null;
        }
        oneElement.next = oneElement.pre = null;
        current_size--;
        return true;
    }
}