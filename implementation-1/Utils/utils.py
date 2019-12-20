import psycopg2
from itertools import combinations

def sortSecondNFResults(secondNFresults):
    firstsFDsCategory=[]
    secondFDCategory=[]
    thirdFDCategory=[]
    fourthFDCategory=[]
    for fdResult in secondNFresults:
        if fdResult<0.5:
            firstsFDsCategory.append(fdResult)
        elif 0.5<=fdResult<=0.75:
            secondFDCategory.append(fdResult)
        elif 0.75<=fdResult<=0.99:
            thirdFDCategory.append(fdResult)
        else:
            fourthFDCategory.append(fdResult)

    return [len(secondNFresults), len(firstsFDsCategory), len(secondFDCategory), len(thirdFDCategory), len(fourthFDCategory)]

def substractLists(list1, list2): 
    return (list(set(list1) - set(list2))) 

def createQueryElements(E):
    queryElement=""
    queryElementList=[]
    for i in range(len(E)): 
        if i==0 or i==len(E):
            queryElement=E[i]
        else:
            queryElement=queryElement+","+E[i]
        queryElementList.append(E[i])
    return [queryElement, queryElementList]