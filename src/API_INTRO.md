\mainpage API Introduction
 Faster defines the [faster](namespacefaster.html) namespace which contains all framework classes and definitions.

 The context class is the class that manages dataset resources and task execution.

 - faster::fastContext class

 The user can create, using the context class several types of distributted datasets:
 - faster::fdd - a dataset of a single type.
 - faster::indexedFdd dataset - a indexed dataset containing a key and a value.
 - faster::groupedFdd dataset class - a group of indexed datasets.

## Step by step

In order to run code using faster you need:

1. Create a context object (faster::fastContext)
2. Register user functions and variables (faster::fastContext::registerFunction)
3. Start worker processes (faster::fastContext::startWorkers)
4. Create a dataset from file or memory (faster::fdd::fdd() or faster::indexedFdd::indexedFdd())
5. Apply your functions to the dataset (faster::fdd::map() or faster::fdd::reduce() etc.)
6. Write the dataset to disk or collect its content (faster::fddCore::writeToFile(), faster::fdd::collect())

## Examples

 - \subpage examples Full working examples


