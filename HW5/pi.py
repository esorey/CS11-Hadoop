#!/usr/bin/python
import findspark
findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf

import sys
from random import random
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    sc = SparkContext(appName="PythonPi")

    # Grab the number of partitions from the command line
    partitions = int(sys.argv[1])
    
    # Make an RDD of random points (x, y), with 0 <= x,y < 1.
    points = sc.parallelize([(random(), random()) for i in xrange(partitions)])

    # Map each point to 0 or 1, based on whether or not it lies in the 
    # quarter circle of radius 1.
    indicators = points.map(lambda x : 1 if x[0]**2 + x[1]**2 <= 1 else 0)

    # Sum the indicators to get a count of the number of points falling
    # within the quarter circle
    in_count = indicators.reduce(add)

    # Print the approximation for pi
    print 4 * float(in_count) / partitions 

#### Hadoop vs Spark
# The biggest difference I found between Spark and Hadoop in writing
# this simple program was how lightweight Spark felt in comparison to
# Hadoop. Rather than having to work with separate classes for mapping
# and reducing as in Hadoop, Spark allows me to do everything in a central
# location. It also handles all of the details of reducing for me.
# For example, in Hadoop, I have to manually implement adding together all of
# the indicators to get a count of points within the quarter circle. With Spark,
# it's just a simple call to reduce.