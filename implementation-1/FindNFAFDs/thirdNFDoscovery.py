import psycopg2
from DatabaseAccess.connect import executeQuerry, connect, closeConnection
from itertools import combinations
from Utils.utils import sortSecondNFResults,createQueryElements,substractLists

consideredAttributesGroups=[]
secondNFresults=[]

def resetGlobalVariables():
    global consideredAttributesGroups
    consideredAttributesGroups=[]
    global secondNFresults
    secondNFresults=[]

def meausre3NFLevel(parameters):
    visitLatticeForThirdNF([], parameters[2], 0, parameters, 3, (len(parameters[2])/3)+1)
    result=sortSecondNFResults(secondNFresults)
    resetGlobalVariables()
    return result

def visitLatticeForThirdNF(A, B, i, parameters, k, step):
    if i>len(B) or k==0:
        return 0
    else: 
        if i+step<len(B):
            A.extend(B[i:i+step])
        else:
            A.extend(B[i:len(B)])
        i = int(i) + step
        findFDForThirdNF(A, parameters)
        visitLatticeForThirdNF (A, B, i, parameters, k-1, step)

def findFDForThirdNF(E, parameters):
    i=1
    while i<3 and i<len(E):
        attributesCombinations = list(combinations(E, i))
        i = int(i) + 1
        for attributeSubset in attributesCombinations:
            if list(attributeSubset) not in consideredAttributesGroups and list(attributeSubset)!=[]:
                queryElements=createQueryElements(list(attributeSubset))  
                executeFDDoscoveryQueryForThirdNF(queryElements[0], queryElements[1], parameters) 

def executeFDDoscoveryQueryForThirdNF(queryElement, queryElementList, parameters):
    consideredAttributesGroups.append(queryElementList)
    for otherNonKeyAttribute in substractLists(parameters[2],queryElementList):
        query="Select(Select Sum(countDup)From(Select "+queryElement+", Count(*) As countB, Sum(freq) As countDup From (Select "+queryElement+", "+otherNonKeyAttribute+", Count(*) As freq From  "+parameters[0]+" Group By "+queryElement+", "+otherNonKeyAttribute+") As F Group By "+queryElement+") As FD Where countB=1)::decimal /(Select Count(*) From  "+parameters[0]+") As VALUE"
        result=executeQuerry(query)[0][0]
        secondNFresults.append(result) 