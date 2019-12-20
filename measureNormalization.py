from FindNFAFDs.findFDMetrics import findFDMetricsPerTable
from DatabaseAccess.connect import executeQuerry, connect, closeConnection
import time
from GenerateGraphsTools.createGraphExecutionTimePerNbAttributes import generateGraph

executionTimeInfoForTheConsideredAttributes=[]
resultsForDataTable=[]
graphTitle="Runtime_VS_#Of_Attributes"
xLabel='Number of attributes'
yLabel='Execution time'

#dataTablesWithTheirCinsideredAttributes=[
#    ["mushroom", ["cap_shape","cap_color","odor","gill_spacing","gill_color","stalk_surface_above_ring","stalk_color_above_ring","veil_type","ring_number","spore_print_color"],
#    ["cap_surface", "bruises", "gill_attachment", "gill_spacing", "gill_size", "stalk_shape", "stalk_root", "stalk_surface_below_ring", "stalk_color_below_ring", "ring_type", "veil_color", "population", "habitat", "class"]
#    ],
#    ["ipumsla99", ["nmothers", "momloc", "steppop", "sploc", "nchlt5", "age", "race", "schltype", "occscore", "ind1950", "hrswork2"],
#    ["year", "gq", "gqtype", "farm", "ownershp", "value", "rent", "ftotinc", "nfams", "ncouples", "nfathers", "poploc", "sprule", "famsize"]
#    ],
#    ["uscensus", ["caseid", "iclass", "ifertil", "dincome2", "dincome8", "imay75880", "iothrserv", "drearning", "irownchld", "ischool"],
#    ["dage", "dancstry1", "dancstry2", "iavail", "icitizen", "ddepart", "idisabl1", "idisabl2", "ienglish", "ifeb55", "dhispanic", "dhour89", "dhours", "iimmigr", "dincome1"]
#    ]
#]

#dataTablesWithTheirCinsideredAttributes=[
#    ["Air_Traffic_Landings_Statistics", ["activity_period", "operating_airline", "operating_airline_IATA_code", "published_airline", "Published_airline_IATA_code", "geo_summary", "geo_region", "landing_aircraft_type", "aircraft_body_type", "aircraft_manufacturer", "aircraft_model", "aircraft_version", "landing_count", "total_landed_weight"],
#    []
#    ]
#]

dataTablesWithTheirCinsideredAttributes=[
    ["mushroom", ["cap_shape","cap_color","odor","gill_spacing","gill_color","stalk_surface_above_ring","stalk_color_above_ring","veil_type","ring_number","spore_print_color"],
    []
    ]
]


def measureExecutionTimePerNumberOfAttributes():
    global resultsForDataTable
    for databaseElement in dataTablesWithTheirCinsideredAttributes:
        saveExecutionTimePerDatabase(databaseElement[0], databaseElement[1]+databaseElement[2])
        executionTimeInfoForTheConsideredAttributes.append([databaseElement[0], resultsForDataTable])
        resultsForDataTable=[]
        
def saveExecutionTimePerDatabase(tableName, consideredAttributes):
    print("------")
    print("Calculating execution time for the table: "+tableName)
    if len(consideredAttributes)>=5:
        print("Processing 5 attributes")
        measureExecutionTime(tableName, consideredAttributes[0:4])
    else:
        resultsForDataTable.append(None)
    if len(consideredAttributes)>=10:
        print("Processing 10 attributes")
        measureExecutionTime(tableName, consideredAttributes[0:9])
    else:
        resultsForDataTable.append(None)
    if len(consideredAttributes)>=15:
        print("Processing 15 attributes")
        measureExecutionTime(tableName, consideredAttributes[0:14])
    else:
        resultsForDataTable.append(None)
    if len(consideredAttributes)>=20:
        print("Processing 20 attributes")
        measureExecutionTime(tableName, consideredAttributes[0:19])
    else:
        resultsForDataTable.append(None)
    if len(consideredAttributes)>=25:
        print("Processing 25 attributes")
        measureExecutionTime(tableName, consideredAttributes[0:24])
    else:
        resultsForDataTable.append(None)

def measureExecutionTime(tableName, consideredAttributes):
    start=time.time()
    findFDMetricsPerTable(tableName, consideredAttributes,0)
    resultsForDataTable.append(time.time()-start)

def constituteGraphElements():
    legendsList=[]
    resultsList=[]
    xList=[0, 5, 10, 15, 20, 25]
    for resultElement in executionTimeInfoForTheConsideredAttributes:
        legendsList.append(resultElement[0])
        resultsList.append(resultElement[1])
    generateGraph(xList, resultsList, legendsList, graphTitle, xLabel, yLabel)


def findNormalizationDegreeForTheConsideredDatabases():
    for databaseElement in dataTablesWithTheirCinsideredAttributes:
        findFDMetricsPerTable(databaseElement[0], databaseElement[1],1)


if __name__ == '__main__':
    connect()
    findNormalizationDegreeForTheConsideredDatabases()
    #measureExecutionTimePerNumberOfAttributes()
    #constituteGraphElements()
    closeConnection()