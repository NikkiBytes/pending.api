 # TISSUES Data Plugin for Biothings Pending API   
  
Data plugin using [TISSUES](https://tissues.jensenlab.org/About) data.  
  
## Notes  
  
  
  
- **Association centric**  document.   
- Currently only human files used as input, _other species are available._

## Example Record
```  

{
    "_id": "CLDB:0007242_0000000090",
    "subject": {
        "id": "CLDB:0007242",
        "name": "COV-644"
    },
    "association": {
        "tissue_name": "COV-644",
        "zscore": "2.239",
        "confidence": "1.120",
        "category": "textmining"
    },
    "object": {
        "ensembl": "hsa-miR-892a",
        "symbol": "hsa-miR-892a"
    }
}
```