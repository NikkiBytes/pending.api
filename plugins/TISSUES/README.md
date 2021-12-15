 # TISSUES Data Plugin for Biothings  
  
Input data is from [TISSUES Database](https://tissues.jensenlab.org/About).  
  
## Notes  
  
  
  
- **Association centric**  document.   
- Can change the `_id` below to its symbolic representation. 

## <u> Document Structure</u>  
  
```  
[
    {
        "_id": "BTO:0000000",
        "TISSUES": {
            "associatedWith": [
                {
                    "ensembl": "18S_rRNA",
                    "symbol": "18S_rRNA",
                    "zscore": 6.111,
                    "confidence": 3.055,
                    "category": "textmining"
                }
            ],
            "tissue_id": "BTO:0000000",
            "tissue_name": "tissues, cell types and enzyme sources"
        }
    },
    {
        "_id": "BTO:0000042",
        "TISSUES": {
            "associatedWith": [
                {
                    "ensembl": "18S_rRNA",
                    "symbol": "18S_rRNA",
                    "zscore": 5.997,
                    "confidence": 2.999,
                    "category": "textmining"
                }
            ],
            "tissue_id": "BTO:0000042",
            "tissue_name": "BTO:0000042"
        }
    },
    .
    .
    .
]

```