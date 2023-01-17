from os import killpg
import pandas as pd
import numpy as np
import ast
import time
import json5 as json
from collections import defaultdict
import argparse 


def dataframe_gen():
    
    df = pd.read_csv('New full.csv')
    df = df.drop(columns=['REG_crsSchool', 'REG_REG_credHr', 'REG_classSize', 'CRS_crsCampus', 'CRS_schdtyp', 'FID', 'CRS_contact_hrs', 'CRS_XLSTGRP', 'CRS_PrimarySect', 'CRS_enrolltally', 'STU_DegreeSeek', 'STU_credstua',])
    df = df.loc[(df['REG_Programcode'] == Dept)] #| (df['REG_Programcode'] == 'CHEM')] #| (df['REG_Programcode'] == 'BISC') | (df['REG_Programcode'] == 'MATH') | (df['REG_Programcode'] == 'PHYS')]
    df['OTCM_FinalGradeN'].replace(' ', np.nan, inplace = True)
    df.dropna(subset=['OTCM_FinalGradeN'], inplace=True)
    #Labs -- droppped 
    
    df = df.drop(df[(df['Coursecode'] == 'CISC1610') | (df['Coursecode'] == 'CISC2010') | (df['Coursecode'] == 'CISC2110') 
                    | (df['Coursecode'] == 'PHYS1511') | (df['Coursecode'] == 'PHYS1512') | (df['Coursecode'] == 'PHYS2010') | (df['Coursecode'] == 'PHYS2011') 
                    | (df['Coursecode'] == 'BISC1413') | (df['Coursecode'] == 'BISC1414') | (df['Coursecode'] == 'BISC2549') | (df['Coursecode'] == 'BISC2571') 
                    | (df['Coursecode'] == 'BISC3142') |(df['Coursecode'] == 'BISC3242') | (df['Coursecode'] == 'BISC3653') | (df['Coursecode'] == 'BISC3231') | (df['Coursecode'] == 'BISC3415')
                    | (df['Coursecode'] == 'CHEM1331') | (df['Coursecode'] == 'CHEM1332')| (df['Coursecode'] == 'CHEM2531')| (df['Coursecode'] == 'CHEM2532')| (df['Coursecode'] == 'CHEM2541')
                    | (df['Coursecode'] == 'CHEM2542')| (df['Coursecode'] == 'CHEM3631') | (df['Coursecode'] == 'CHEM3632') | (df['Coursecode'] == 'CHEM4231')| (df['Coursecode'] == 'CHEM4432')].index)
    df = df.drop(df[(df['OTCM_FinalGradeC'] == 'W') | (df['OTCM_FinalGradeC'] == 'INC')| (df['OTCM_FinalGradeC'] == 'HPCE') 
                | (df['OTCM_FinalGradeC'] == 'PCE')].index)
    df = df.reset_index(drop=True)
    
    sorted_df = df.sort_values(by = ['termOrder', 'Coursecode'], ascending=True)
    sorted_df = sorted_df.groupby(['SID']).agg({'Coursecode': lambda x: list(x),'termOrder':lambda x: list(x)}).reset_index()
    sorted_df = sorted_df.drop(columns=['SID'])
    
    transactions = len(sorted_df.index) + 1
    sorted_df.to_csv('transactions_df.csv')
    
    return transactions

def insert_delimitor(df):
    course = [i.strip("[]").split(", ") for i in df.Coursecode]
    semester = [i.strip("[]").split(", ") for i in df.termOrder]

    K_itemset = []
    updated_elem1 = []
    part1 = " "
    
    for i in range(len(semester)):
        start = 0
        elem1 = course[i]
        term1 = semester[i]
        item = term1[0]
        if len(term1) == 1:              #only one course
            K_itemset.append(elem1[0])
        else:
            if len(set(term1)) == 1:    #all courses in same semester 
                #print(term1)
                part1 = ','.join(elem1)
                updated_elem1.append(part1)
            else:
                for j in range(0, len(term1)-1):
                    item1 = float(term1[j])
                    item2 = float(term1[j+1])
                    if item1 < item2:
                        #print("==========")
                        #print(item1)
                        #print(item2)
                        #print("==========")
                        part1 = ','.join(elem1[start:j+1])
                        start = j + 1
                        updated_elem1.append(part1)
                part1 = ','.join(elem1[start:len(elem1)])
                updated_elem1.append(part1)
                
            item = '|'.join(updated_elem1)
            #print("===========")
            #print(item)
            #print(term1)
            #print("===========")
            #print(item)
            K_itemset.append(item)
            updated_elem1.clear()
            
    #for i in range(len(K_itemset)):
        #print( K_itemset[i])
    d = {'Coursecode': K_itemset}
    new_df = pd.DataFrame(d)
    
    new_df.to_csv('transactions_delimiter.csv')
    #print(new_df.Coursecode)
    
    return(K_itemset) 
    

# ITEMSET JOIN
def itemset_join(itemset):
    unionset = []
    res = [i.strip("[]").split("|") for i in itemset]
    res2 = [i.strip("[]").replace("|", ",").split(",") for i in itemset]

    for i in range(len(res)):
        element1 = res[i]
        elem1 = res2[i]
        total = len(element1)
        for k in range(i+1, len(res)):     
            element2 = res[k]
            elem2 = res2[k]
            index = 0
            join_items = []
            join_items2 = []
            join_items3 = []
            status = 0
            sub_status = 0
            if len(element1) == len(element2):
                for j in range(len(element1)):
                    block1 = element1[j]
                    block1= block1.strip("[]").split(",")             #block 1 
                    block2 = element2[j]
                    block2= block2.strip("[]").split(",")             #block 2 
                    
                    if len(block1) == len(block2): #If both blocks are equal 
                        if j == len(element1) -1:  # If final block 
                            if block1[0:(len(block1)-1)] == block2[0:(len(block1)-1)]: #If in final block all but last course match
                                index += 1
                        else:    
                            if block1[0:(len(block1))] == block2[0:(len(block1))]: # If all courses match in block
                                index += 1
                
                # If number of matched blocks == number of total blocks, then perform join   
                # Three Join Cases: 
                # Case 1: a,b,c  -- a,b,d  --> a,b,c,d
                # Case 2: a,b,c  -- a,b|d  --> a,b,c|d
                # Case 3: a,b|c  -- a,b|d  --> a,b|c,d AND a,b,c|d AND a,b,d|c
                           
                if index == total: 
                    block_items = []
                    joined_block = []
                    unsort_block = []
                    
                    #Example: 1000, 1100| 1200, 1300, 1400  -- 1000, 1100| 1200, 1300, 1600 --> 1000, 1100| 1200, 1300, 1400, 1600
                    unsort_block.append(block1[-1])                  #Last item of itemset 1 - ex) 1400 
                    unsort_block.append(block2[-1])                  #Last item of itemset 2 - ex) 1600
                    sort_block = ','.join(sorted(unsort_block))      #Join together with ',' and sort - ex) 1400, 1600 
                    
                    list1 = '|'.join(element1[0: len(element1)-1])   #Join Element1 back together minus the last block -- ex) 1000, 1100
                    join_items.append(list1)                         
                    
                    if len(block1) > 1:  #If the last block has multiple items ex) a| b, c 
                        joined_block = ','.join(block1[0: len(block1)-1]) #Join the last block minus the last item -- ex) 1200, 1300
                        block_items.append(joined_block)                  #Add last block to block_items
                        
                        
                    block_items.append(sort_block)                 #Add the last two joined items to block_items
                    final_block = ','.join(block_items)            #Join block_items with a comma -- ex) 1200, 1300, 1400, 1600
                    
                    join_items.append(final_block)                 
                    result = '|'.join(join_items)                   #ex) -- 1000, 1100|1200, 1300, 1400, 1600
                    
                    unionset.append(result.rstrip(',').lstrip('|'))             #add to list of return items 
                    
                    
                    #Case 2: last delimotor is '|'  ex) elem1: a, b | c  -- elem2: a, b | d 
                    if len(block1) == 1:      #last block has only one item 
                        #print("TRUE")
                        part1 = '|'.join(res[i])  #join together all of elem1
                        join_items2.append(part1)  #add elem1 to join_items
                        join_items2.append(block2[-1])  #single element of final block2
                        result2 = '|'.join(join_items2) #result1 == a, b | c | d
                        unionset.append(result2.rstrip(',').strip('"'))
                    
                        status = 1 
                        sub_status = 1
                       
            #equal number of elements but different number of semesters 
            #EX: itemset1: a, b, c | e   --- itemset2: a, b, c, d --- result: a, b, c, d | e 
            elif len(elem1) == len(elem2) and (len(element1)-len(element2) == 1 or len(element2)-len(element1) == 1):
                if len(element1) > len(element2):
                    for j in range(len(element1)-1):   #all except last item/block
                        block1 = element1[j]
                        block1= block1.strip("[]").split(",")
                        
                        if index == (len(element1) -2):
                            block2 = element2[j]
                            block2= block2.strip("[]").split(",")
                            if block1[0:(len(block1))] == block2[0:(len(block2)-1)]:
                                block1 = element1[j+1]
                                if block1 != block2[-1]:
                                    index += 1                
                        else:
                            block2 = element2[j]
                            block2= block2.strip("[]").split(",")
                            if len(block1) == len(block2) and block1[0:(len(block1))] == block2[0:(len(block2))]:
                                index += 1                    
                    if index == len(element1)-1:
                        status = 1
                        sub_status = 1
                        block1 = element1[-1].strip("[]").split(",")
                # element2 = '1400|2000' AND element1 = '1400,1600' 
                elif len(element2) > len(element1):
                    for j in range(len(element2)-1):   #all except last item/block
                        block1 = element2[j]
                        block1= block1.strip("[]").split(",")
                        if index == (len(element2) -2):
                            block2 = element1[j]
                            block2= block2.strip("[]").split(",")
                            if block1[0:(len(block1))] == block2[0:(len(block2)-1)]:
                                block1 = element2[j+1]
                                if block1 != block2[-1]:
                                    index += 1                
                        else:
                            block2 = element1[j]
                            block2= block2.strip("[]").split(",")
                            if len(block1) == len(block2) and block1[0:(len(block1))] == block2[0:(len(block2))]:
                                index += 1                    
                    if index == len(element2)-1:
                        status = 1
                        sub_status = 2
                        block1 = element2[-1].strip("[]").split(",")    
            ##EX: itemset1: a, b, c | e   --- itemset2: a, b, c, d --- result: a, b, c, d | e 
            if status == 1: 
                if sub_status == 1:
                    part2 = '|'.join(res[k])   #join together all of elem2 (a,b,c,d)
                elif sub_status == 2:
                    part2 = '|'.join(res[i])   #join together all of elem1
                join_items3.append(part2)      #add elem2 to join2_items
                join_items3.append(block1[-1]) #single element of final (e)
                result3 = '|'.join(join_items3)  # (a,b,c,d|e)
                unionset.append(result3.rstrip(',').strip('"'))  
                #print("==============")
                #print(itemset[i])
                #print(itemset[k])
                #print(result3)
                #print("===============")
    return unionset        


def candidate_prune(count, minsupport):         
    itemset = []
    for i in count:
        if count[i] >= minsupport:
            #print(i,':',count[i])
            itemset.append(i)     
    return itemset

## COUNTS AMOUNT OF SETS THAT CONTAINS CANDIDATE SETS IN SAME ORDER ##
## PARAM: CANDIDATE SUBSETS, LENGTH OF ITEMSET, AND DF ## 
def count_subset(candidate, length, df):
    Lk = defaultdict(int)
    res = [i.split("|") for i in df]
    candidate_set = [i.replace(" ","").split("|")  for i in candidate]   
   
    for row in range(len(res)):
        data = res[row] 
        for item in range(0, length):  #iterate through the dataframe         
            item1 = candidate_set[item]                         
            z = 0 
            counter = 0
            if len(data) >= len(item1):
                for i in range(len(item1)):        
                    block1= item1[i]                
                    block1 = block1.split(",")
                    for j in range(z, len(data)):   
                        block2 = data[j]
                        block2 = block2.split(",")
                        if len(block2) >= len(block1):
                            w = 0
                            sub_counter = 0
                            for k in range(len(block1)):     #for each block in item1
                                for l in range(w, len(block2)):      #for each block in data 
                                    if block1[k] == block2[l]:       # if the course matches 
                                        sub_counter += 1             #incremeent counter 
                                        if w != len(block2):         #Don't increment to next object if you're on the last one  
                                            w = l + 1                    #move starting position to the next item in block2 
                                        break
                            if sub_counter == len(block1):
                                counter +=1 
                                if z != len(data):
                                    z = j + 1 
                                break
                if counter == len(item1):
                    #print("=======")
                    #item1_print = '|'.join(item1)
                    #data_print =  '|'.join(data)
                    #print(item1_print)
                    #print(data_print)
                    #print("=======")
                    key = (candidate[item])
                    Lk[key] +=1                           
    print("COMPLETE")
    return Lk                
                            
def run_Apriori(Ck, minsupport, k, df): 
    dict = {}
    
    while Ck != []:                                         ## While loop keeps running algorithm until no more freq sets can get generated ## 
        K = str(k) 
        col = "Freq " + K + "-Itemsets"
        count = count_subset(Ck, len(Ck), df)               ## Count subset indexurrences to be pruned ## 
        
        
        #print(count)
        
        freq_set = candidate_prune(count, minsupport)           ## Prunes subsets that don't reach threshold ## 
        #for i in range(len(freq_set)):
            #print(freq_set[i])
        
        Ck = itemset_join(freq_set)                             ## Generate k+1 itemset ## 
        #for i in range(len(Ck)):
            #print(Ck[i])
        
        if Ck != []:                                  ## To ensure no empty k-itemsets are added to dict
            dict[col] = count
        
        k += 1
        print("Iteration Complete")
    return dict

    

## EXPORTS DATAFRAME TO CSV ##
## PARAM: FINAL DICT TO CHANGE TO CSV, MINSUPPORT TO REMOVE ROWS ## 
## MINSUPPORT IS NEEDED TO REMOVE UNWANTED ROWS, THIS IS DIFFERENT FROM CANDIDATE_PRUNE FUNCTION ##
def dataframe_tocsv(dict, minsupport, transactions):
    #dict = [i.replace('"','') for i in dict]
    export_df = pd.DataFrame(dict)
   
    for column in export_df:
        export_df.drop(export_df[export_df[column] < minsupport].index, inplace = True)
    
    k_transactions = export_df.count()
    k_transactions = k_transactions.to_dict()
    print(k_transactions)
    export_df['Count %'] = (export_df.sum(axis = 1) / transactions) * 100
    
    #print(export_df)
    export_df.to_csv(export_file_name)
    return k_transactions


## EXPORTS ADDITIONAL RESULTS TO A TEXT FILE ##
## PARAM: COUNT OF SINGLE ITEMS, COUNT OF N-SIZE TRANSACTIONS, NUMBER OF TRANSACTIONS, ALGO RUNTIME ## 
def export_data(single, set, transactions, runtime):
    transactions = str(transactions)
    runtime = str(runtime)
    with open('Export.txt', 'w') as file:
        file.write(json.dumps(single))
        file.write('\n' + '\n')
        file.write(json.dumps(set))
        file.write('\n' + '\n')
        file.write("Transaction #: " + transactions)
        file.write('\n' + '\n')
        file.write("--- %s seconds ---" % runtime)              




# ========================================= MAIN =================================================== #

## GENERAL VARIABLES ## 
file_name = 'CISC_TRAIL_1-1'
start_time = time.time()

#INITIALIZING PARSER

parser = argparse.ArgumentParser(
                    description = 'Process arguments'
                    )
parser.add_argument('--file_name', type = str, required = True)
parser.add_argument('--minsup', type = int, required = True)
parser.add_argument('--department', type = str, required = True)
args, unknown = parser.parse_known_args()
export_file_name = args.file_name
minsupport = args.minsup
Dept = args.department
##

## GENERATING DATAFRAME ##
transactions = dataframe_gen()

## READING IN PROCESSED DATAFRAME ##
df = pd.read_csv('transactions_df.csv')

#INSERTING DELIMITER
new_df = insert_delimitor(df)

## ESTABLISHING MIN SUPPORT AND PASS index ##
#minsupport = #
k = 2

## FIRST PASS ##
## FIRST PASS IS OUTSIDE OF LOOP TO GENERATE FREQ SINGLES ##
single_count = defaultdict(int)
set_count = defaultdict(int)


row = [i.strip("[]").replace(", ", ",").split(",") for i in df.Coursecode]

for i in range(len(row)):
    
    elem = row[i]
    key = len(row[i])      # k- sized  itemset 
    set_count[key] += 1    # number of k - sized itemset 
    for item in range(len(elem)):
        single_count[elem[item]] += 1                     
#print(single_count)
#print(set_count)

freq_singles = candidate_prune(single_count, minsupport)

# print ('====================================')
# print ('Frequent 1-itemset is',freq_singles)
# print ('====================================')
# print("Transaction #: ", transactions)

Ck = itemset_join(freq_singles)
#print(Ck)

## RUNS APRIORI ALGORITHM ## 

export_dict = run_Apriori(Ck, minsupport, k, new_df)
k_count = dataframe_tocsv(export_dict,  minsupport, transactions)
session = (time.time() - start_time)
export_data(single_count, k_count, transactions, session)