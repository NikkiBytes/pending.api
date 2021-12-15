import biothings.utils.dataload as dl   
from multiprocess import Pool, Manager
import requests, json
import time
from itertools import groupby
from operator import itemgetter

# set up multiprocessing 
manager=Manager()
symbol_dict=manager.dict()

SYMBOL_RESOLVE_RESULT = {}

def fetch_symbol(original_input):
    if original_input in SYMBOL_RESOLVE_RESULT:
        return SYMBOL_RESOLVE_RESULT[original_input]
    if original_input.startswith("hsa-"):
        #print("original_input", original_input)
        if original_input.endswith("p"):
            mygene_input = original_input.rsplit("-", 1)[0]
        else:
            mygene_input = original_input
        try:
            res = requests.get(
                "http://mygene.info/v3/query?q=alias:{alias}&fields=symbol".replace("{alias}", mygene_input)).json()
        except:
            return None
        if "hits" in res and len(res["hits"]) > 0:
            #print("output", res["hits"][0]['symbol'])
            SYMBOL_RESOLVE_RESULT[original_input] = res['hits'][0]['symbol']
            return res['hits'][0]['symbol']
        else:
            SYMBOL_RESOLVE_RESULT[original_input] = None
            return None
    elif original_input.startswith("ENSP"):
        res = requests.get(
            "http://mygene.info/v3/query?q=ensembl.protein:{alias}&fields=symbol".replace("{alias}", original_input)).json()
        if "hits" in res and len(res["hits"]) > 0:
            #print("output", res["hits"][0]['symbol'])
            SYMBOL_RESOLVE_RESULT[original_input] = res['hits'][0]['symbol']
            return res['hits'][0]['symbol']
        else:
            SYMBOL_RESOLVE_RESULT[original_input] = None
            return None
    elif original_input.startswith("ENSG"):
        res = requests.get(
            "http://mygene.info/v3/query?q=ensembl.gene:{alias}&fields=symbol".replace("{alias}", original_input)).json()
        if "hits" in res and len(res["hits"]) > 0:
            #print("output", res["hits"][0]['symbol'])
            SYMBOL_RESOLVE_RESULT[original_input] = res['hits'][0]['symbol']
            return res['hits'][0]['symbol']
        else:
            SYMBOL_RESOLVE_RESULT[original_input] = None
            return None
    elif "." in original_input:
        return None
    else:
        return original_input


def load_tm_hu_data(datalist, symbol_dict):
    """
        Load human tissue text mining data
        Input:
        - filepath: input tsv file
        returns:
        - json_docs: list of records generated from input file
    """
    json_docs=[]
    for row in datalist:
        row_dict={}
        row_dict['ensembl']=row[0]
        row_dict['symbol']=symbol_dict[row[1]]
        row_dict['tissue_identifier']=row[2]
        row_dict['tissue_name']=row[3]
        row_dict['zscore'] = float(row[4])
        row_dict['confidence'] = float(row[5])
        row_dict['category'] = 'textmining'
        json_docs.append(row_dict)
        #print("[INFO] row dict: ", json.dumps(row_dict, indent=4))
    return json_docs;

def symbol_search(id_):
    """ 
    Function to search on given id for a corresponding symbol, and 
    then held in a dictionary for future reference.
    Keyword arguments:
        id_: given id to search on
    """
    try:
        symbol_dict[id_]=fetch_symbol(id_)
    except:
        symbol_dict[id_]=None

def load_data(data_folder):
    """ 
    Main data load function
    Keyword arguments:
        data_folder -- folder storing downloaded files
    """
    orig_st=time.time()
    records=[]
    tm_path=os.path.join(data_folder,"human_tissue_textmining_full.tsv" )
    #tm_path = "/Users/nacosta/Documents/data/human_tissue_textmining_full.tsv" # input file

    # set up symbol dict
    # get the data tissue ids and convert them into their symbol and
    # load into a dictionary that can be referenced later on.
    # saves us time in processing downstream
    print("[INFO] loading TISSUE data ....")
    ts=time.time()
    datalist=dl.tab2list(tm_path, (0,1,2,3,4,5,6)) # load into biothings
    tissue_ids=list(set([x[1] for x in datalist])) # get unique list of ids
    print("[INFO] loaded data into biothings list: {:0.02f} seconds.".format(time.time()-ts))
    print("[INFO] datalist is size: %s docs/rows."%len(datalist))

    # set up multiprocessing 
    ts=time.time()
    p=Pool()
    p.map(symbol_search, tissue_ids)
    print("[INFO] Processing in parallel time: {:0.02f} seconds.".format(time.time()-ts))
    print("[INFO] %s dictionary keys made. "%len(symbol_dict.keys()))

    # extract data
    json_docs=load_tm_hu_data(datalist, symbol_dict)
    for key, group in groupby(json_docs, key=itemgetter('tissue_identifier')):
        res = {
            "_id": None,
            "TISSUES": {
                "associatedWith": []
            }
        }
        merged_doc = []
        for _doc in group:

            #print(_doc)
            #if key.startswith("BTO:"):
            res["_id"] = key
            res["TISSUES"]['tissue_id'] = key               
            #else:
                #print(key)
                #continue
            _doc.pop("tissue_identifier")
            res["TISSUES"]["tissue_name"] = _doc.pop("tissue_name")
            merged_doc.append(_doc)
        res['TISSUES']['associatedWith'] = merged_doc
    
    print("[INFO] Finished making records, total time: {:0.2f} seconds.".format(time.time()-orig_st))
    print("[INFO] example record: \n", json.dumps(res, indent=4))
    print("[INFO] PROCESS COMPLETE.")

    return records;
