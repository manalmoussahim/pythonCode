import psycopg2
from DatabaseAccess.connect import executeQuerry, connect, closeConnection
from itertools import combinations
from primaryKeyDiscovery import executeAlgorithmToExtractMostReleventPK
from secondNFDiscovery import meausre2NFOrBCLevel
from Utils.utils import substractLists
from thirdNFDoscovery import meausre3NFLevel
import time
import numpy as np
from prettytable import PrettyTable

def findFDMetricsPerTable(tableName, testAttributes, displayResults):
    secondNFResults=[]
    thirdNFResults=[]
    bcNFResults=[]
    primaryKey=executeAlgorithmToExtractMostReleventPK(tableName, testAttributes)
    if displayResults==1:
        printPKResult(tableName, primaryKey[0])
    if primaryKey!=[]:
        nonPrimaryKeyAttributes=substractLists(testAttributes,primaryKey[0])
        if len(primaryKey[0])>1:
            secondNFResults=meausre2NFOrBCLevel([tableName,primaryKey[0], nonPrimaryKeyAttributes, 1])
        if len(nonPrimaryKeyAttributes)>1:
            thirdNFResults=meausre3NFLevel([tableName,primaryKey[0], nonPrimaryKeyAttributes])
        bcNFResults=meausre2NFOrBCLevel([tableName,primaryKey[0], nonPrimaryKeyAttributes, 0])
        if displayResults==1:
            printNFResults(secondNFResults, thirdNFResults, bcNFResults)

def printPKResult(tableName, primaryKey):
    print("________________________________________________________________________")
    print("Results for table "+tableName)
    print("The table's primary key is: ")
    print(np.array(primaryKey))

def printNFResults(secondNFResults, thirdNFResults, bcNFResults):
    resultsToDisplay=[]
    if(secondNFResults!=[]):
        secondNFResults.insert(0, "2NF")
        resultsToDisplay.append(secondNFResults)
    if(thirdNFResults!=[]):
        thirdNFResults.insert(0, "3NF")
        resultsToDisplay.append(thirdNFResults)
    if(bcNFResults!=[]):
        bcNFResults.insert(0, "BCNF")
        resultsToDisplay.append(bcNFResults)
    displayTable(resultsToDisplay)

def displayTable(resultsToDisplay):
    header=["Normal Form", "Total AFD", "AFD < 0.5", "0.5 <= AFD < 0.75 ", "0.75 <= AFD <= 0.99 ", "AFD = 1"]
    resultsTable = PrettyTable(header)
    for resultToDisplay in resultsToDisplay:
        resultsTable.add_row(resultToDisplay)
    print(resultsTable)

def print2NFResults(normalForm, results):
    print("_______________")
    print("The total number of FDs considering the "+normalForm+" is: "+str(results[0]))
    print("The total number of FDs less than 0.5 is: "+str(results[4]))
    print("The total number of FDs between 0.5 and 0.75 is: "+str(results[3]))
    print("The total number of FDs between 0.75 and 0.99 is: "+str(results[2]))
    print("The total number of FDs=1 is: "+str(results[1]))