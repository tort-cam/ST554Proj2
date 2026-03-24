## My python script file for Project 2
## Author: Cameron Mullaney

## In this file, I create a class titled "SparkDataCheck", which will take in either a pandas df or a csv and produce a new object with that data frame as an attribute.
## This script also features a variety of methods to be used on this data.

import numpy as np
import pandas as pd
import math
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.types import NumericType

class SparkDataCheck :
    def __init__(self, df):
        self.df = df
        
## You can create a SparkDataCheck object using a CSV or a Pandas DF

## First is from a CSV:
    @classmethod
    def fromcsv(cls, fileaddress, spark):
        
        """Creates a Spark DF while reading in a CSV file"""
        s = spark.read.load(
            fileaddress,
            format = "csv",
            sep = ",",
            inferSchema = True,
            header = True
        )
        return cls(s)

## Next is from a Pandas DF:
    @classmethod
    def frompdf(cls, pdf, spark):
        
        """Creates a Spark DF from a pandas DF"""
        p = spark.CreateDataFrame(pdf)
        return cls(p)
    
#### VALIDATION METHODS ####

    def withinlimits(self, colname, lower = float('nan'), upper = float('nan')):
        """
        Checks if values in a specified column are within the supplied limits
        """
        ## Check if any limits were supplied

        if lower == float('nan') and upper == float('nan'):
            raise ValueError('withinlimits: no limits supplied - No actions taken')
            return self

        ## Create our booleans to see if supplied limits are in an acceptable format

        haslow = isinstance(lower, (int, float)) and not math.isnan(lower)
        hasup = isinstance(upper, (int, float)) and not math.isnan(upper)

        ## Pull some info from the supplied column, and test for numeric column

        field = self.df.schema[colname]
        typecheck = isinstance(field.dataType, NumericType)

        ## Testing our booleans from above

        if haslow == False and hasup == False:
            raise ValueError('withinlimits: no valid limits supplied - No actions taken')
            return self

        if typecheck == False:
            print ("Non-numeric column supplied - No actions taken")
            return self

        ## Running our actual method, testing column values based on supplied limits

        if haslow and hasup:
            self.df = self.df.withColumn(colname + " within (" + str(lower) + "," + str(upper) + ")", self.df[colname].between(lower, upper))

        elif hasup:
            self.df = self.df.withColumn(colname + " below " + str(upper), self.df[colname] <= upper)

        elif haslow:
            self.df = self.df.withColumn(colname + " above "+ str(lower), self.df[colname] >= lower)

        return self

    ## Check String List ##
    
    def onlist(self, colname, levels):
        """
        Checks if string values appear on a given list
        """
        
        ## Have to check that 'levels' is either a string 
        
        levelscheck = isinstance(levels, StringType) or all(isinstance(x, str) for x in levels)
        if levelscheck == False:
            print ("Non-string list supplied - No actions taken")
            return self
        
        ## Pulling data from supplied column

        field = self.df.schema[colname]
        typecheck = isinstance(field.dataType, StringType)
        
        ## Quick test for proper data type

        if typecheck == False:
            print ("Non-string column supplied - No actions taken")
            return self

        ## Testing to see if column values are on the "levels" list, and appending boolean column

        self.df = self.df.withColumn(colname + " on list", self.df[colname].isin(levels))

        return self

    ## Test if Null ##
    
    def nulltest(self, colname):
        """
        Marks rows with a NULL value
        """
        
        ## Testing if cells are "NULL"

        self.df = self.df.withColumn(colname +" is null", self.df[colname].isNull())

        return self

    #### SUMMARIZATION METHODS ####
    
    ## Min Max ##

    def minmax(self, colname = None, groupvar = None):
        """
        Create a pandas df with the minimum and maximum values of each column
        Option to group by an additional column
        """
        
        ## Boolean for if a column was specified
        
        print(type(colname))
        colsupp = not colname is None
        groupsupp = not groupvar is None
        
        ## Printing some method info
        
        print ("Column Provided: " + str(colsupp))
        print ("Grouping Var Provided: " + str(groupsupp))
        
        ## If we are working with a single, specified column:
        
        if colsupp:
            
            ## Pulling data from supplied column

            field = self.df.schema[colname]
            typecheck = isinstance(field.dataType, NumericType)
            
            ## Testing column data type
            
            if typecheck == False:
                print ("Non-numeric column supplied - No actions taken")
                return self
            
            ## If we have a grouping variable:
            
            if groupsupp:
                end = self.df.groupBy(groupvar).agg(F.min(colname), F.max(colname)).toPandas()
                return end
            else:
                end = self.df.select(colname).agg(F.min(colname), F.max(colname)).toPandas()
                return end
        
        ## If no column supplied:
        
        else:
            
            old = pd.DataFrame()
            t = 0
            
            ## Iterate through each column
            
            for x in self.df.columns:
                
                ## Check if the column is numeric
                
                field = self.df.schema[x]
                colcheck = isinstance(field.dataType, NumericType)
                
                ## If so, summarize it
                
                if colcheck:
                    
                    ## If a grouping variable is specified:
                    ## t checks if its our first time through, and adds the column for the groupvar
                    
                    if groupsupp:
                        
                        if t == 0:
                            new = self.df.groupBy(groupvar).agg(F.min(x), F.max(x)).toPandas()
                        else:
                            new = self.df.groupBy(groupvar).agg(F.min(x), F.max(x)).drop(groupvar).toPandas()
                    
                    ## If no grouping variable is specified
                    
                    else:
                        new = self.df.select(x).agg(F.min(x), F.max(x)).toPandas()
                    
                    ## Add new line to minmax df
                    
                    old = pd.concat([old, new], axis = 1)
                    t+=1
                
            ## Here, I am transposing the pandas df to be more readable.
            
            return old.T
                    
    ## Counting String Occurrences ##        
            
    def strcount(self, colname, colname2 = None):
        """
        Returns a pandas df with the count of each level of column of strings
        Optional - Provided second column will result in count of each combination of levels from each column
        """
        
        c2supp = not colname2 is None
        field = self.df.schema[colname]
                
        ## Quick tests for proper data type

        typecheck = isinstance(field.dataType, StringType)
        
        if typecheck == False:
            
            print ("Non-string first column supplied - No actions taken")
            
            return self
        
        ## If a second column is provided:
        
        if c2supp:
            
            ## Check if it's numeric
            
            field2 = self.df.schema[colname2]
            typecheck2 = isinstance(field2.dataType, StringType)
            
            if typecheck2 == False:
                
                print ("Non-string second column supplied - No actions taken")
                
                return self
            
            ## Create a pandas df of counts of combinations
            
            f = self.df.groupBy(colname, colname2).count().toPandas().sort_values(colname)
        
        else:
            
            ## If only one column, create pandas df of counts
            
            f = self.df.groupBy(colname).count().toPandas()
        
        return f
    
    
## Tadaa ##