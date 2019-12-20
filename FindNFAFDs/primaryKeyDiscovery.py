import psycopg2
from DatabaseAccess.connect import executeQuerry, connect, closeConnection
from itertools import combinations
from Utils.utils import createQueryElements, substractLists

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
        assignPrimaryKeyLevelForLattices(A, B)
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

def assignPrimaryKeyLevelForLattices(E, B):
    i=1
    while i<=3 and i<len(E):
        attributesCombinations = list(combinations(E, i))
        i = int(i) + 1
        for attributeSubset in attributesCombinations:
            if list(attributeSubset) not in consideredAttribtesGroups:
                queryElements=createQueryElements(list(attributeSubset))
                nonKeyQueryElements=createQueryElements(substractLists(B,list(attributeSubset)))[0]
                executePKDiscoveryQuery(queryElements[0], queryElements[1], nonKeyQueryElements) 
            
def executePKDiscoveryQuery(queryElement, queryElementList, nonKeyQueryElements):
    consideredAttribtesGroups.append(queryElementList)
    #query="Select(Select Count(Distinct("+queryElement+")) From "+tableName+" Where ("+queryElement+") is not Null)::numeric /(Select Count(*) From "+tableName+") As VALUE"
    query="select (select count(*)  from (select "+queryElement+" from "+tableName+" group by "+queryElement+" having count(distinct("+nonKeyQueryElements+"))=1) as DerivedTable)::numeric/ (select count(distinct("+queryElement+")) from "+tableName+") As VALUE"
    result=executeQuerry(query)[0][0]
    if result=="None":
        result=0
    consideredAttributeGroupsWithPKMetricValue.append([queryElementList, result])
    primaryKeysConsideredSubsets.append(result)

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
    if len(tableAttributes)>1:
        visitLatticeForPrimaryKey([], attributes, 0, 3, (len(attributes)/3)+1)
    else:
        executePKDiscoveryQuery(tableAttributes[0], tableAttributes, tableAttributes[0])
    result=getMostRelevantPKSubset()
    resetGlobalValues()
    return result

if __name__ == '__main__':
    connect()
    result=executeAlgorithmToExtractMostReleventPK("test", ["k1", "k2", "a", "b", "c"])
    print(result)

