import psycopg2
from DatabaseAccess.connect import executeQuerry, connect, closeConnection
from itertools import combinations
from Utils.utils import sortSecondNFResults,createQueryElements

tableName=""
primaryKey=[]
nonKeyAttributes=[]
consideredPrimaryKeyGroups=[]
secondNFresults=[]
fdsElementAfterFiltering=[]
fdsElements=[]

def removeDuplicatedFDs():
    for fdElement in fdsElements:
        res=ifFDIsDuplicated(fdElement)
        if res==1:
            fdsElementAfterFiltering.append(fdElement)

def ifFDIsDuplicated(fdElement):
    for otherFDElement in fdsElements:
        if fdElement[1]==otherFDElement[1]:
            if ifFDElementInOtherFDElements(fdElement[0], otherFDElement[0])==1  and fdElement[0]!=otherFDElement[0]:
                return 0
    return 1

def ifFDElementInOtherFDElements(fdElement, otherFDElement):
    for singleOtherFDElement in otherFDElement:
        if singleOtherFDElement not in fdElement:
            return 0
    return 1

def meausre2NFOrBCLevel(parameters):
    visitLatticeForSecondNFOrBCNF([], parameters[1], 0, 3, parameters)
    removeDuplicatedFDs()
    i=0
    while i<len(fdsElementAfterFiltering):
        secondNFresults.append(1)
        i=i+1
    result=sortSecondNFResults(secondNFresults)
    resetGlobalValues()
    return result

def resetGlobalValues():
    global nonKeyAttributes
    nonKeyAttributes=[]
    global consideredPrimaryKeyGroups
    consideredPrimaryKeyGroups=[]
    global secondNFresults
    secondNFresults=[]
    global fdsElementAfterFiltering
    fdsElementAfterFiltering=[]
    global fdsElements
    fdsElements=[]

def visitLatticeForSecondNFOrBCNF(A, B, i, k, parameters):
    if i>=len(B) or i>=3:
        return 0;
    else: 
        A.append(B[i])
        i = int(i) + 1
        findFDForNF(A, parameters)
        visitLatticeForSecondNFOrBCNF (A, B, i, k, parameters)
    
def findFDForNF(E, parameters):
    i=1
    while i<=3 and i<=len(E):
        attributesCombinations = list(combinations(E, i-1))
        i = int(i) + 1
        for attributeSubset in attributesCombinations:
            if list(attributeSubset) not in consideredPrimaryKeyGroups and list(attributeSubset)!=[]:
                queryElements=createQueryElements(list(attributeSubset))  
                executeFDDoscoveryQueryForNF(queryElements[0], queryElements[1], parameters) 

def executeFDDoscoveryQueryForNF(queryElement, queryElementList, parameters):
    consideredPrimaryKeyGroups.append(queryElementList)
    for nonKeyAttribute in parameters[2]:
        if parameters[3]==1:
            #query="Select(Select Sum(countDup)From(Select "+queryElement+", Count(*) As countB, Sum(freq) As countDup From (Select "+queryElement+", "+nonKeyAttribute+", Count(*) As freq From  "+parameters[0]+" Group By "+queryElement+", "+nonKeyAttribute+") As F Group By "+queryElement+") As FD Where countB=1)::decimal /(Select Count(*) From  "+parameters[0]+") As VALUE"
            query="select (select count(*)  from (select "+queryElement+" from "+parameters[0]+" group by "+queryElement+" having count(distinct("+nonKeyAttribute+"))=1) as DerivedTable)::numeric/ (select count(distinct("+queryElement+")) from "+parameters[0]+") As VALUE"
        elif parameters[3]==0:
            #query="Select(Select Sum(countDup)From(Select "+nonKeyAttribute+", Count(*) As countB, Sum(freq) As countDup From (Select "+nonKeyAttribute+", "+queryElement+", Count(*) As freq From  "+parameters[0]+" Group By "+nonKeyAttribute+", "+queryElement+") As F Group By "+nonKeyAttribute+") As FD Where countB=1)::decimal /(Select Count(*) From  "+parameters[0]+") As VALUE"
            query="select (select count(*)  from (select "+nonKeyAttribute+" from "+parameters[0]+" group by "+nonKeyAttribute+" having count(distinct("+queryElement+"))=1) as DerivedTable)::numeric/ (select count(distinct("+nonKeyAttribute+")) from "+parameters[0]+") As VALUE"
        result=executeQuerry(query)[0][0]
        if result!=1 and result!=0:
            secondNFresults.append(result) 
        elif result==1:
            if parameters[3]==1:
                fdsElements.append([queryElementList, nonKeyAttribute])
            elif parameters[3]==0:
                fdsElements.append([nonKeyAttribute, queryElementList])