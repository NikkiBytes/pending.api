import biothings.utils.dataload as dl   
import  json, glob
import time, os
from itertools import groupby
from operator import itemgetter


def load_tm_data(datafiles):
    """
        Load human tissue text mining data
        Input:
        - filepath: input tsv file
        returns:
        - json_docs: list of records generated from input file
    """

    json_docs = []

    for file in datafiles:
        datalist=dl.tab2list(file, (0,1,2,3,4,5,6)) # load into biothings

        for row in datalist:
            row_dict={}
            row_dict['ensembl']=row[0]
            row_dict['symbol']=row[1]#symbol_dict[row[1]]
            row_dict['tissue_identifier']=row[2]
            row_dict['tissue_name']=row[3]
            row_dict['zscore'] = row[4]
            row_dict['confidence'] = row[5]
            row_dict['category'] = 'textmining'
            json_docs.append(row_dict)
            #print("[INFO] row dict: ", json.dumps(row_dict, indent=4))
    return json_docs



def load_ep_kn_data(datafiles, category):
    """ load data from the experiments or knowledge file
    note: the experiments file and the knowledge file share the same column names
    Keyword arguments:
    file_path -- the file path of the experiments or knowledge file
    category -- the category of the file, should be either experiments or knowledge
    """

    json_docs = []

    for file in datafiles:
        datalist=dl.tab2list(file, (0,1,2,3,4,5,6)) # load into biothings

        for row in datalist:
            row_dict={
                "ensembl":row[0],
                "symbol": row[1],
                "tissue_identifier": row[2],
                "tissue_name": row[3],
                "source": row[4], 
                "expression": row[5],
                "confidence": float(row[6]),
                "category": category
            }
            json_docs.append(row_dict)
            #print(json.dumps(row_dict, indent=4))
    return json_docs


def load_data(data_folder):
    """ 
    Main data load function
    Keyword arguments:
    data_folder -- folder storing downloaded files
    """
    orig_st=time.time()
    records=[]
    print("[INFO] Loading TISSUE data ....")

    kn_files=glob.glob(os.path.join(data_folder, "*_tissue_knowledge_full.tsv"))
    print("[INFO] %s knowledge files loaded."%(len(kn_files)))#, kn_files))
    tm_files=glob.glob(os.path.join(data_folder, "*_tissue_textmining_full.tsv"))
    print("[INFO] %s text mining files loaded."%(len(tm_files)))#, tm_files))
    ex_files=glob.glob(os.path.join(data_folder, "*_tissue_experiments_full.tsv"))
    print("[INFO] %s experiments files loaded."%(len(ex_files)))#, ex_files))
    
    # extract data
    json_docs=load_tm_data(tm_files) + load_ep_kn_data(ex_files,  "experiments")+load_ep_kn_data(kn_files, "knowledge")
    for key, group in groupby(json_docs, key=itemgetter('tissue_identifier')):
        res = {
            "_id": None,
            "subject": {},
            "association":{},
            "object":{}            
        }

        merged_doc = []

        for _doc in group:
            res["_id"] = key
            res["subject"]['id'] = key               
            _doc.pop("tissue_identifier")
            res["subject"]["name"] = _doc.pop("tissue_name")
            merged_doc.append(_doc)
        for key in merged_doc[0].keys():
            if key == "ensembl" or key == "symbol":
                res['object'][key]=merged_doc[0][key]
            else:
                res["association"][key]= merged_doc[0][key]

        records.append(res)
    
    print("[INFO] Finished making records, total time: {:0.2f} seconds.".format(time.time()-orig_st))
    print("[INFO]  %s records made. "%len(records))
    print("[INFO] Example records \n", json.dumps(records[-4:], indent=4))
    return records

load_data("/Users/nacosta/Documents/data/") 