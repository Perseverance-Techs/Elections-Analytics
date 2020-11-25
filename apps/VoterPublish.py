import time
import requests
import pandas as pd 
import re
import sys

if __name__ == "__main__":
    try:
        print(sys.argv[1:])
        print("The thread number is--------------------->"+str(int(sys.argv[1])))
        voterDataframe = pd.read_csv(r"/home/ElectionProject/in-progress/CleansedVoters_"+ str(int(sys.argv[1]))+".csv",encoding='utf-8-sig')
        outboundPath= "/home/ElectionProject/outbound/processed_"+ str(int(sys.argv[1]))+".csv"
        schemaDF= pd.read_csv("/home/ElectionProject/config/Schema-Mapping.csv",encoding='utf-8')
        print("<--------------------------Printing the schema Manager DF------------------->"+schemaDF)    
        print("<-------------------------The columns in Schema  DF are--------------------------->"+schemaDF.columns) 
        
        
        processedDF = pd.read_csv(outboundPath,encoding='utf-8')
        threadManagerDF= pd.read_csv("/home/ElectionProject/config/ThreadManager.csv",encoding='utf-8')
        
        print("The columns in thread Manager DF are "+str(threadManagerDF.columns)) 
        print("The columns in Voter Manager DF are "+str(voterDataframe.columns)) 
        
        threadManagerDF= threadManagerDF.loc[threadManagerDF['Thread_ID'] == int(sys.argv[1])]
        processStartRow=int(threadManagerDF['RowID_Start'].values[0])
        processEndRow=int(threadManagerDF['RowID_end'].values[0])
        
        print("The start row is--------------->"+str(processStartRow))
        print("The end row is--------------->"+str(processEndRow))    
        print(threadManagerDF)
        
        #voterDataframe.to_csv (r'/home/ElectionProject/outbound/processedVoters.csv', index = False, header=True)
        voterDataframe=voterDataframe.loc[(voterDataframe['ROW_NUMBER'] >= int(processStartRow)) & (voterDataframe['ROW_NUMBER'] <= int(processEndRow))]
        #voterDataframe = voterDataframe[int(processStartRow) : int(processEndRow), :]




        print(str(voterDataframe.columns.values))
        voterDataRefined= voterDataframe[['id_eng','fname_local','age','sex','house_number_local','guardian_name_local']]
        print(str(voterDataRefined.columns.values))

        voterJsonDF=voterDataRefined
        voterJsonDF.columns = ['IDNumber','name','age','Sex','Housename','Fathername']
        print(str(voterJsonDF.columns.values))
        voterJsonDF.age = voterJsonDF.age.astype(str)
        processedDF['ID_JOIN'] = processedDF['IDNumber']
        print("The columns in processed DF are"+str(processedDF.columns))

        voterMerged = pd.merge(voterJsonDF, processedDF, on='IDNumber', how='left')
        voterMerged = voterMerged[voterMerged['ID_JOIN'].isna()]
        print(str(voterMerged.columns.values))
        print("The first 50 rows in the merged voter dataframe is"+str(voterMerged.head(50)))

        voterMerged= voterMerged[:30]
        currentDF=voterMerged['IDNumber']
        processedDF = processedDF['IDNumber']
        df_concat = processedDF.append(currentDF)

        voterJSON=voterMerged.drop(['ID_JOIN','IDNumber'],axis=1)



        voterJSON=voterMerged
        voterJSON=voterJSON.drop(['ID_JOIN'],axis=1)

        json= voterJSON.to_json(orient='records')

        url = "https://perselectionsapi.pagekite.me/publishKafka"
        #requests.post(url,json,timeout=5)

        df_concat.to_csv (outboundPath, index = False, header=True)
        print(str(json))
    except:
        print("And exception occured")


