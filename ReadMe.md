#Perquisites
* Scala 2.11.5
* Spark 2.4.4
* Make


#Input Arguments
The code takes 3 input arguments

1. Path of the input data file

2. Degree of relation that needs to determined

3. Output path to store the results


#Implementation Notes:

The code is implemented using Graphframes this is keeping in mind a large dataset 
,there is trade-off while running graphframes for smaller datasets.


The friends of different degrees are computed in advance so there is a tradeoff in space however 
from there on  any degree of relationship can be obtained in o(n).

##Usage
```bash
make input_file=./src/input_data.txt degree=2 app-submit
```

The above compiles, runs unit tests and creates a package jar for spark submit.
Finally it does spark-submit with given inputs

