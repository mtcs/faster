#  Operator Groups

Operators can be divided by behaviour and variants, but also, there are special operator reserved for grouped datasets.

There are four main operator behaviour:

- [Update](group__update.html)
- [Map](group__map.html)
- [FlatMap](group__flatmap.html)
- [Reduce](group__reduce.html)

Also, there are two variants:

- [Bulk](group__bulk.html)
- [ByKey](group__bykey.html)

Also, when two or more datasets are grouped together, some functions listed before can be used:

- [Grouped](group__grouped.html)




## @defgroup update Update Operators

@brief Run a iterative update operaton

@param K - Key type of the created dataset
@param T - Value type of the source dataset
@param L - Key type of the created dataset
@param U - Value type of the created dataset

@param funcP - A function pointer of a user function _void F(T&)_ that
will be used on each dataset entry

@return A pointer to a new dataset




## @defgroup map Map Operators

@brief Run a __n to n__ map operaton

@param K - Key type of the created dataset
@param T - Value type of the source dataset
@param L - Key type of the created dataset
@param U - Value type of the created dataset

@param funcP - A function pointer of a user function _U F(T&)_ that
will be used on each dataset entry

@return A pointer to a new dataset




## @defgroup flatmap FlatMap Operators

@brief Run a __n to m__ flatMap operation

@param K - Key type of the created dataset
@param T - Value type of the source dataset
@param L - Key type of the created dataset
@param U - Value type of the created dataset

@param funcP - A function pointer of a user function _deque<T> F(T,T)_ that
will be used on each dataset entry

@return A pointer to a new dataset




## @defgroup reduce Reduce Operators

@brief Run a __n to 1__ reduce

@param K - Key type of the created dataset
@param T - Value type of the source dataset

@param funcP - A function pointer of a user function _T F(T,T)_ that
will be used to summarize values

@return summarized value of type T





## @defgroup bulk Bulk Operator Variants

@brief A variant of original operators that receive multiple entries of a dataset at the same
time.

Bulk operators use user functions that can access multiple entries of the local dataset at the
same time _U F(T*, size\_t)_.

@param K - Key type of the created dataset
@param T - Value type of the source dataset
@param L - Key type of the created dataset
@param U - Value type of the created dataset

@param funcP - A function pointer of a user function _U F(T&)_ that
will be used on each dataset entry

@return A pointer to a new dataset




## @defgroup bykey ByKey Operator Variants

@brief A variant of original operators that groups entries by key to be processed.

ByKey operators use user functions that can access multiple entries of the same corresponding key
_U F(K, vector<void*>, size\_t)_.

@param K - Key type of the created dataset
@param T - Value type of the source dataset
@param L - Key type of the created dataset
@param U - Value type of the created dataset

@param funcP - A function pointer of a user function _U F(K, vector<void*>, size\_t)_ that
will be used on each dataset entry

@return A pointer to a new dataset



## @defgroup memmodel Memory Model

@brief Automatic memory deallocation

In order to allow for operator chains like this:

@code{.c}
...
int result = someFdd -> map(&myMap) -> flatMap(&myFlatMap) -> reduce(&myReduce);
...
@endcode

a automatic memory deallocation model was adopted. If a user apply some operators to a dataset, its
distributed memory will be deallocated. In order to use a dataset more than once, the user needs to
protect his dataset with the cache() function and discard its content once it is done with the
_discard()_ function.

@return pointer to self



## @defgroup grouped Grouped Datasets Operators

@brief Once the user run a indexedFdd::cogroup a grouped dataset will be created.

The grouped dataset created is a lightweight object that wrapps existing datasets in order to offer
more complex operations.

@param K - Key type of the created dataset
@param T - Value type of the source dataset
@param L - Key type of the created dataset
@param U - Value type of the created dataset

@param funcP - A function pointer of a user function _U F(K, vector<void*>, size\_t)_ that
will be used on each dataset entry

@return A pointer to a dataset group




## @defgroup shuffle Shuffle Related Operations

@brief dataset entry exchange between machines.

The groupByKey() and cogroup() operations performa shuffle of information between machines in the
cluster. The group locally in each machine every element of a dataset that has the same key. Shufle
operations are usually associated with network operations because in order to group elements by key
in the cluster, all machines have to send data that does not belong to it to the propper owner.

Note that when a dataset is grouped by key, the key location data is saved to be reused. That way,
when calling cogroup multiple times, execution time is saved.

@code{.cpp}
...
auto g1 = data.cogroup(data2); <--- this will take longer
auto g2 = data.cogroup(data3); <--- now it will take less time
...
@endcode

@return pointer to self
