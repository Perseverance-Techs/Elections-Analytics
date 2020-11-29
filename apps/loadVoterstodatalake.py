import time
import requests
import pandas as pd 
import re
import sys

if __name__ == "__main__":
    try:
        #Get the thread Number that is passed to the main Method.
        print(sys.argv[1:])
        print("The thread number is--------------------->"+str(int(sys.argv[1])))

        #Append the thread number to fetch the corresponding inbound CSV file.
        voterDataframe = pd.read_csv(r"/home/ElectionProject/in-progress/CleansedVoters_"+ str(int(sys.argv[1]))+".csv",encoding='utf-8-sig')
        outboundPath= "/home/ElectionProject/outbound/processed_"+ str(int(sys.argv[1]))+".csv"


        #Read the thread Manager and get the start and end configurations.
        threadManagerDF= pd.read_csv("/home/ElectionProject/config/ThreadManager.csv",encoding='utf-8')        
        threadManagerDF= threadManagerDF.loc[threadManagerDF['Thread_ID'] == int(sys.argv[1])]
        processStartRow=int(threadManagerDF['RowID_Start'].values[0])
        processEndRow=int(threadManagerDF['RowID_end'].values[0])     
        seviceURL=str(threadManagerDF['ServiceURL'].values[0])         
        print("The start row is--------------->"+str(processStartRow))
        print("The end row is--------------->"+str(processEndRow))    
        print(threadManagerDF)

        #Fetch the processed files from the outbound path.This is to ensure that duplicates are not published to the datalake.
        processedDF = pd.read_csv(outboundPath,encoding='utf-8')


        #Fetch the corresponding schema mapping file
        schemaDF= pd.read_csv(r"/home/ElectionProject/config/Schema-Mapping.csv")
        print("<--------------------------Printing the schema Manager DF------------------->"+str(schemaDF))    
        print("<-------------------------The columns in Schema  DF are--------------------------->"+str(schemaDF.columns))         
        schemaMapping= schemaDF[['Source_column','Target_Column']]        
        listVoterCols= voterDataframe.columns
        mappedCols=[]
        sourceCols=[]
        print("The source and the target columns in the mapping are"+str(schemaMapping))

        #Get the List of source Columns and the corresponding list of target columns.
        for n,colName in enumerate(listVoterCols):
            schemaRow= schemaMapping.loc[schemaMapping['Source_column'] == colName]
            print("Looking up the column in the schemaMapping"+str(colName))
            print("Printing the schema Row"+str(schemaRow))

            mapCount = int(schemaRow.shape[0])
            print("The mapCount is"+str(mapCount))
            
            if mapCount==1:
               mappedCol= schemaRow.iloc[0]['Target_Column']
               mappedCols.append(mappedCol)
               sourceCols.append(colName)
            else:
               print("This particular column in the CSV file is not mapped")
               
      
        
        #voterDataframe.to_csv (r'/home/ElectionProject/outbound/processedVoters.csv', index = False, header=True)
        voterDataframe=voterDataframe.loc[(voterDataframe['ROW_NUMBER'] >= int(processStartRow)) & (voterDataframe['ROW_NUMBER'] <= int(processEndRow))]
        #voterDataframe = voterDataframe[int(processStartRow) : int(processEndRow), :]

        print(str(voterDataframe.columns.values))
        voterDataRefined= voterDataframe[['id_eng','fname_local','age','sex','house_number_local','guardian_name_local']]

        
        #Start Mapping the Dataframe--Create the source dataframe
        print("#Start Mapping the Dataframe--Create the source dataframe,the source Columns are "+str(sourceCols))
        print("#Start Mapping the Dataframe--Create the target dataframe,the target Columns are "+str(mappedCols))
        voterDataSouce= voterDataframe[sourceCols]

        print("#Start Mapping the Dataframe--Create the target dataframe")
        print("The columns in the source dataframe are"+str(voterDataSouce.columns))
        voterDataTarget=voterDataSouce
        voterDataTarget.columns= mappedCols
        print("The columns in the target dataframe are"+str(voterDataTarget.columns))

        
        #Do the necessary Transformations on the VoterJSON Dataframe.
        voterJsonDF=voterDataTarget
        print("The columns in the voterJSON DF are"+str(voterJsonDF.columns.values))
        voterJsonDF.VOTER_AGE = voterJsonDF.VOTER_AGE.astype(str)

        #Create a new column in the processed DF called ID Join.
        #The processed DF is used to ensure that duplicates are not published.       
        processedDF['ID_JOIN'] = processedDF['IDNumber']
        print("The columns in processed DF are"+str(processedDF.columns))
        voterMerged = pd.merge(voterJsonDF, processedDF, on='IDNumber', how='left')
        #This line excludes all the rows in the VoterDataframe that are already published to the Datalake.
        voterMerged = voterMerged[voterMerged['ID_JOIN'].isna()]
        print("The first 10 rows in the merged voter dataframe is"+str(voterMerged.head(10)))


        #Take the first 30 lines of the voter Merged Dataframe to create the JSON file. Write the same to the outbound so that 
        #these dont get published again.

        voterMerged1= voterMerged[10:15]
        voterMerged2= voterMerged[5:10]
        voterMerged= voterMerged[0:5]

        currentDF=voterMerged['IDNumber']
        processedDF = processedDF['IDNumber']
        df_concat = processedDF.append(currentDF)


        voterJSON=voterMerged.drop(['ID_JOIN','IDNumber'],axis=1)
        voterJSON=voterMerged
        voterJSON=voterJSON.drop(['ID_JOIN'],axis=1)
        voterJSON=voterJSON.fillna("NA")
        print("The first 10 rows in the voterJSON Dataframe is"+str(voterJSON.head(10)))
        print("The columns in the voter JSON are"+str(voterJSON.columns))
        json= voterJSON.to_json(orient='records')
        jsonString=str(json)
        PARAMS = {'jsonData':jsonString} 
        r= requests.get(seviceURL,params = PARAMS,timeout=5)
        print("The response is--------------------------->"+str(r))
        df_concat.to_csv (outboundPath, index = False, header=True)


        currentDF=voterMerged1['IDNumber']
        df_concat = processedDF.append(currentDF)

        voterJSON=voterMerged1.drop(['ID_JOIN','IDNumber'],axis=1)
        voterJSON=voterMerged1
        voterJSON=voterJSON.drop(['ID_JOIN'],axis=1)
        voterJSON=voterJSON.fillna("NA")
        print("The first 10 rows in the voterJSON Dataframe is"+str(voterJSON.head(10)))
        print("The columns in the voter JSON are"+str(voterJSON.columns))
        json= voterJSON.to_json(orient='records')
        jsonString=str(json)
        PARAMS = {'jsonData':jsonString} 
        r= requests.get(seviceURL,params = PARAMS,timeout=5)
        print("The response is--------------------->"+str(r))

        currentDF=voterMerged2['IDNumber']
        df_concat = df_concat.append(currentDF)

        voterJSON=voterMerged2.drop(['ID_JOIN','IDNumber'],axis=1)
        voterJSON=voterMerged2
        voterJSON=voterJSON.drop(['ID_JOIN'],axis=1)
        voterJSON=voterJSON.fillna("NA")
        print("The first 10 rows in the voterJSON Dataframe is"+str(voterJSON.head(10)))
        print("The columns in the voter JSON are"+str(voterJSON.columns))
        json= voterJSON.to_json(orient='records')
        jsonString=str(json)
        PARAMS = {'jsonData':jsonString} 
        r= requests.get(seviceURL,params = PARAMS,timeout=8)
        print("The response is---------------------------->"+str(r))
        df_concat.to_csv (outboundPath, index = False, header=True)

 
        print(str(json))
    except Exception as e:
        print("An exception occured"+str(e))




