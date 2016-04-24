Classes for operating on vector clocks and dealing with distributed state, directly copied from the Linkedin's [Voldemort](www.project-voldemort.com) project.

## Scalar Clocks

The building block of vector clocks systems is the **scalar logical clock**, implemented in the class [`ClockEntry`](src/main/java/voldemort/versioning/ClockEntry.java). A base clock has an identifier `nodeId` (non-negative short) and a clock value `Version` (positive long).

The important methods of `ClockEntry` objects are:
* constructors,
* `getNodeId()` and `setNodeInt(short)` to get/set the clock id,
* `getVersion()` and `setVersion(long)` to get/set the clock value,
* `equals()` to compare two clocks,
* `incremented()` to return a new clock with incremented clock value,
* `toString()` for debugging purposes.

## Vector Clocks

Scalar clocks are organized in [`VectorClock`](src/main/java/voldemort/versioning/VectorClock.java) objects, representing a **vector logical clock**. It does not use the [`ClockEntry`](src/main/java/voldemort/versioning/ClockEntry.java) class, since the vector of logical clocks is stored sparely in a `Map<Short,Long>` object, since, in general, writes will be mastered by only one node. This means implicitly all the versions are at zero, but we only actually store those greater than zero.

Since vector clocks can be used to derive the *happen before* relation, we define the [`Version`](src/main/java/voldemort/versioning/Version.java) interface, that allows us to determine if a given version happened before or after another version. This could have been done using the comparable interface but that is confusing, because the numeric codes are easily confused, and because concurrent versions are not necessarily "equal" in the normal sense.
```java
public interface Version {
    /**
     * Return whether or not the given version preceded this one, succeeded it, or is concurrent with it 
     * @param v The other version
     */
    public Occurred compare(Version v);
}
```
The [`Occurred`](src/main/java/voldemort/versioning/Occurred.java) return type is a simple enum:
```java
public enum Occurred { BEFORE, AFTER, CONCURRENTLY}
```
The important methods of `VectorClock` objects are:
* constructors,
* compare(version),
* `getMaxVersion()` to retrive the greatest clock value,
* `equals()` to compare two clocks,
* `incremented(node,time)` to return a new clock with `time` on index `node`,
* `incrementVersion(node)` to update the clock with `time` on index `node`,
* `merge(clock)`to return a new clock obtained by *merging* two clocks, selecting the greatest value for every scalar clock present on *at least one* of the two vector clocks,
* `toString()` for debugging purposes.
 
For the sake of debugging, the provided implementation associates to a vector clock a timestamp, generated using system-provided (local) physical time.

## Versioned Objects

Versioned objects of a given type `T` are wrapped in [`Versioned<T>`](src/main/java/voldemort/versioning/Versioned.java) objects, associating a vector logical clock to a given object of class `T`. Two versioned objects must be comparable. Such a comparison must return whether or not the given version preceded this one, succeeded it, or is concurrent with it.

It is implemented by a static class [`HappenedBeforeComparator<T>`](src/main/java/voldemort/versioning/Versioned.java) implementing the [`Comparator<Versioned<S>>`](https://docs.oracle.com/javase/8/docs/api/java/util/Comparator.html) interface.

## Conflicts Resolution

A last set of objects deals with vector clock inconsistencies.  Applications can implement this to provide a method for reconciling conflicts that cannot be resolved simply by the version information.

The interface of such objects is [`InconsistencyResolver<T>`](src/main/java/voldemort/versioning/InconsistencyResolver.java):
```java
public interface InconsistencyResolver<T> {
    public List<T> resolveConflicts(List<T> items);
}
```
which takes two different versions of an object and combine them into a single version of the object.

Implementations must maintain the contract that:

1. `resolveConflict([null, null]) == null`, and
2. if `t != null`, then `resolveConflict([null, t]) == resolveConflict([t, null]) == t`.

Implemented conflict resolvers may, for example:

* arbitrarily resolve the inconsistency by choosing the first object if there is one (see [`ArbitraryInconsistencyResolver<T>`](src/main/java/voldemort/versioning/ArbitraryInconsistencyResolver.java));
* does not attempt to resolve inconsistencies, but instead just throws an exception if one should occur (see [`FailingInconsistencyResolver<T>`](src/main/java/voldemort/versioning/FailingInconsistencyResolver.java));
* resolve inconsistencies based on the physical timestamp in the vector clock (see [`TimeBasedInconsistencyResolver<T>`](src/main/java/voldemort/versioning/TimeBasedInconsistencyResolver.java));
* use the object `VectorClocks` leaving only a set of concurrent versions remaining (ideally 1) (see [`VectorClockInconsistencyResolver<T>`](src/main/java/voldemort/versioning/VectorClockInconsistencyResolver.java)).

