import psycopg2
from DatabaseAccess.connect import executeQuerry, connect, closeConnection
from itertools import combinations
from Utils.utils import createQueryElements

attributes=[]
tableName=""
consideredAttribtesGroups=[]
primaryKeysConsideredSubsets=[]
consideredAttributeGroupsWithPKMetricValue=[]
primaryKeysSubsets=[]

def visitLatticeForPrimaryKey(A, B, i, k, step):
    if i>len(B) or k==0:
        return 0
    else: 
        if i+step<len(B):
            A.extend(B[i:i+step])
        else:
            A.extend(B[i:len(B)])
        i = int(i) + step
        assignPrimaryKeyLevelForLattices(A)
        visitLatticeForPrimaryKey (A, B, i, k-1, step)


def resetGlobalValues():
    global consideredAttribtesGroups
    consideredAttribtesGroups=[]
    global primaryKeysConsideredSubsets
    primaryKeysConsideredSubsets=[]
    global consideredAttributeGroupsWithPKMetricValue
    consideredAttributeGroupsWithPKMetricValue=[]
    global primaryKeysSubsets
    primaryKeysSubsets=[]

def assignPrimaryKeyLevelForLattices(E):
    i=1
    while i<=3 and i<len(E):
        attributesCombinations = list(combinations(E, i))
        i = int(i) + 1
        for attributeSubset in attributesCombinations:
            if list(attributeSubset) not in consideredAttribtesGroups:
                queryElements=createQueryElements(list(attributeSubset))
                executePKDiscoveryQuery(queryElements[0], queryElements[1]) 
            
def executePKDiscoveryQuery(queryElement, queryElementList):
    consideredAttribtesGroups.append(queryElementList)
    query="Select(Select Count(Distinct("+queryElement+")) From "+tableName+" Where ("+queryElement+") is not Null)::numeric /(Select Count(*) From "+tableName+") As VALUE"
    result=executeQuerry(query)[0][0]
    if result=="None":
        result=0
    consideredAttributeGroupsWithPKMetricValue.append([queryElementList, result])
    primaryKeysConsideredSubsets.append(result)


def createPKDoscoveryQuery(E):
    queryElement=""
    queryElementList=[]
    for i in range(len(E)): 
        if i==0 or i==len(E):
            queryElement=E[i]
        else:
            queryElement=queryElement+","+E[i]
        queryElementList.append(E[i])
    executePKDiscoveryQuery(queryElement, queryElementList)


def getMostRelevantPKSubset():
    maximumMetric=max(primaryKeysConsideredSubsets)
    if maximumMetric==0:
        return []
    for subsetElement in consideredAttributeGroupsWithPKMetricValue:
        if subsetElement[1] == maximumMetric:
            return subsetElement

def executeAlgorithmToExtractMostReleventPK(table, tableAttributes):
    global attributes
    attributes=tableAttributes
    global tableName
    tableName=table
    visitLatticeForPrimaryKey([], attributes, 0, 3, (len(attributes)/3)+1)
    result=getMostRelevantPKSubset()
    resetGlobalValues()
    return result