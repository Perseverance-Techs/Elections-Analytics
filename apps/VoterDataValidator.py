import time
import requests
import pandas as pd 
from ml2en import ml2en
import sys
import datetime
currentTime = str(datetime.datetime.now())
currentTime =currentTime.strip()

voterDataframe = pd.read_csv(r"/home/ElectionProject/inbound/Voter-Data-inbound.csv",encoding='utf-8-sig')
print(voterDataframe.index)
voterDataframe['ROW_NUMBER'] = voterDataframe.reset_index().index

businessRulesDF = pd.read_csv(r"/home/ElectionProject/config/Business-Rules.csv",encoding='utf-8-sig')
print("The business Rule DF is------------------>"+str(businessRulesDF.head(10)))
voterDataframe['ERROR_CODE']='NA'
print("The number of rows and columns in the dataframe are=================>"+str(voterDataframe.shape[0]))
converter = ml2en()

def convert_mal_eng(malString):
    try:
   	 return converter.transliterate(malString)
    except:
         return ''
    
for index, row in businessRulesDF.iterrows():

    fieldname = str(row['Field1'])
    strOperator = str(row['Operator'])
    value1 = str(row['Value1'])
    value2 = str(row['Value2'])
    errorCode=str(row['ErrorCode'])
    resultField=str(row['ResultField'])
    
    print("The filed on which the rule is to be applied-----------------------------> "+str(row['Value1']))
    print("The value2 field is  -----------------------------> "+str(row['Value2']))        
    print("The Business Rule Operator is ----------------------------->"+str(strOperator))    
    print("The Error Code in the business Rule is -----------------------------> "+str(row['ErrorCode']))  
    print("Both strings are equal for ,"+str(strOperator)+","+str(strOperator=='ReplaceRegex'))
    if strOperator.strip()=='ReplaceRegex': 
        print("<--------------------->Inside the replaceRegex Operator<-------------------->")    
        voterDataframe[fieldname] = voterDataframe[fieldname].str.replace(value1, value2)
        print("The number of rows and columns in the dataframe ReplaceRegex execution=================>"+str(voterDataframe.shape[0]))        
       
    if strOperator.strip()=='numericRange': 
        print("<--------------------->Inside the numericRange Operator<-------------------->")    
        print("The number of rows and columns in the dataframe ReplaceRegex execution=================>"+str(voterDataframe.shape[0]))            
        voterDataframe[fieldname] = pd.to_numeric(voterDataframe[fieldname], errors='coerce')
        #voterDataframe = voterDataframe.dropna(subset=[fieldname])
        nullAge= voterDataframe.loc[(voterDataframe[fieldname].isnull())]    
        nullAgeNa= voterDataframe.loc[(voterDataframe[fieldname].isna())]           
        voterDataframe.loc[(voterDataframe[fieldname].isnull()),fieldname]=1
        print(nullAge.head(10))
        print(nullAgeNa.head(10))
        voterDataframe[fieldname].fillna(0).astype(int)
        
        voterDataframe[fieldname]  = voterDataframe[fieldname].astype(int) 
        voterDataframe.loc[(voterDataframe[fieldname] > float(value1)) & (voterDataframe[fieldname] < float(value2)) ,row['ResultField']] = row['Resultvalue'] 
        print("<--------------------->The value of the errorCode is<-------------------->"+str(errorCode))          
        if (str(errorCode)!='nan'):
            print("The Error Code is having a valid Value")
            voterDataframe.loc[(((voterDataframe[fieldname] < float(value1)) | (voterDataframe[fieldname] > float(value2)))|(voterDataframe[fieldname]==1)) ,'ERROR_CODE'] = voterDataframe['ERROR_CODE']+'|'+errorCode
 
    if strOperator.strip()=='validateLength': 
        print("<--------------------->Inside the numericRange Operator<-------------------->")    
        print("The number of rows and columns in the dataframe ReplaceRegex execution=================>"+str(voterDataframe.shape[0]))     
        print("<--------------------->The value of the errorCode is<-------------------->"+str(errorCode))          
        if (str(errorCode)!='nan'):
            print("The Error Code is having a valid Value")
            voterDataframe.loc[(voterDataframe[fieldname].str.len()> int(value1)) ,'ERROR_CODE'] = voterDataframe['ERROR_CODE']+'|'+errorCode
        voterDataframe.loc[(voterDataframe[fieldname].str.len()> int(value1)),fieldname] = voterDataframe[fieldname].str.slice(0,int(value1)) 
        
    if strOperator.strip()=='--convertMaltoEng': 
        print("<--------------------->Inside the replaceRegex Operator<-------------------->")    
        voterDataframe[value1] = voterDataframe[fieldname].apply(convert_mal_eng)      

    if strOperator.strip()=='isNull': 
        print("<--------------------->Inside the replaceRegex Operator<-------------------->")    
        voterDataframe.loc[voterDataframe[fieldname].isnull(),'ERROR_CODE'] = voterDataframe['ERROR_CODE']+'|'+errorCode
errorDataframe = voterDataframe.loc[(voterDataframe['ERROR_CODE']!='NA')]
errorDataframe.to_csv (r'/home/ElectionProject/errors/recErrors_'+currentTime+'.csv',index = False, header=True)   
#errorDataframe.to_csv (r'/home/ElectionProject/error/recErrors.csv',index = False, header=True)     
voterDataframe.to_csv (r'/home/ElectionProject/in-progress/CleansedVoters.csv', index = False, header=True)