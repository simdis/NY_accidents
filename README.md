# NY_accidents
Some examples of using both python and Hadoop to find out some properties, defined by a university project.

The python examples are developed using the pandas library (http://pandas.pydata.org), whereas the corresponding Hadoop scripts are developed as a Map-Reduce job.

The retrieved information are:
1) The number of weekly lethal accidents.
2) The number of accidents per contributing factor and for each of them the percentage of fatal accidents. Note that the contributing factors of each involved vehicle is considered separately and that the "Unspecified" factor is considered only below the column corresponding to the first vehicle (not always there are 5 vehicles involved).
3a) The number of weekly accidents per borough.
3b) The average weekly accidents per borough

NOTE: The dataset is not available into this repository. Actually, it can be downloaded at https://data.cityofnewyork.us/Public-Safety/NYPD-Motor-Vehicle-Collisions/h9gi-nx95
