import psycopg2
from DatabaseAccess.connect import executeQuerry, connect, closeConnection
from itertools import combinations
from Utils.utils import sortSecondNFResults

consideredAttributesGroups=[]
bcNFresults=[]
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

def resetGlobalVariables():
    global consideredAttributesGroups
    consideredAttributesGroups=[]
    global bcNFresults
    bcNFresults=[]
    global fdsElementAfterFiltering
    fdsElementAfterFiltering=[]
    global fdsElements
    fdsElements=[]

def meausreBCNFLevel(parameters):
    visitLatticeForBCNF([], parameters[2], 0, parameters, 3, (len(parameters[2])/3)+1)
    removeDuplicatedFDs()
    i=0
    while i<len(fdsElementAfterFiltering):
        bcNFresults.append(1)
        i=i+1
    result=sortSecondNFResults(bcNFresults)
    resetGlobalVariables()
    return result

def visitLatticeForBCNF(A, B, i, parameters, k, step):
    if i>len(B) or k==0:
        return 0
    else: 
        if i+step<len(B):
            A.extend(B[i:i+step])
        else:
            A.extend(B[i:len(B)])
        i = int(i) + step
        findFDForBCNF(A, parameters)
        visitLatticeForBCNF (A, B, i, parameters, k-1, step)

def findFDForBCNF(E, parameters):
    i=0
    while i<=3 and i<len(E):
        attributesCombinations = list(combinations(E, i))
        i = int(i) + 1
        for attributeSubset in attributesCombinations:
            if list(attributeSubset) not in consideredAttributesGroups and list(attributeSubset)!=[]:
                createFDDoscoveryQueryForBCNF(list(attributeSubset), parameters)



def createFDDoscoveryQueryForBCNF(E, parameters):
    queryElement=""
    queryElementList=[]
    for i in range(len(E)): 
        if i==0 or i==len(E):
            queryElement=E[i]
        else:
            queryElement=queryElement+","+E[i]
        queryElementList.append(E[i])
    executeFDDoscoveryQueryForBCNF(queryElement, queryElementList, parameters)

def executeFDDoscoveryQueryForBCNF(queryElement, queryElementList, parameters):
    consideredAttributesGroups.append(queryElementList)
    for pkAttribute in parameters [1]:
        #query="Select(Select Sum(countDup)From(Select "+queryElement+", Count(*) As countB, Sum(freq) As countDup From (Select "+queryElement+", "+pkAttribute+", Count(*) As freq From  "+parameters[0]+" Group By "+queryElement+", "+pkAttribute+") As F Group By "+queryElement+") As FD Where countB=1)::decimal /(Select Count(*) From  "+parameters[0]+") As VALUE"
        query="select (select count(*)  from (select "+queryElement+" from "+parameters[0]+" group by "+queryElement+" having count(distinct("+pkAttribute+"))=1) as DerivedTable)::numeric/ (select count(distinct("+queryElement+")) from "+parameters[0]+") As VALUE"
        result=executeQuerry(query)[0][0]
        if result!=1 and result!=0:
            bcNFresults.append(result) 
        elif result==1: 
            fdsElements.append([queryElementList, pkAttribute])
