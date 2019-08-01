<h1 style="color:#483D8B;"> Facebook Ad Objects and Insights  - API Consumption </h>

### Year 2019
### Developed by **Sarbadal Pal**

---

<p style="color:#BDB76B;font-size:20px;"> 
The purpose of this tool is to automate the process of complex data pull. All params and credentials
are automatically downloaded from a pre-defined S3 buckets.
</p>

> **Features of this tool are --**
>- User can customized the what needs to pulled just by changing / editing json file in S3 location
>- User can add data pull just by adding parameters in a json file
>- Credentials folder location and 
>- data dropped location can be changed without developer's intervention
>- Users will be able to change the FB App's credentials when it expires

## Project structure...
```
project
│   README.md
│   .gitignore    
│
└───credentils
│   │   __init__.py
│   │   adobjectsfields.json  # This file holds the fields. This file will be replaced by downloading from S3
|   |   credentials.json      # This file holds the credentials of both S3 and FB App. This file will be replaced by downloading from S3
│   
└───func
|   │   __init__.py
|   │   func.py               # Blank file. Will be removed / updated in future
|
└───params
|   |   __init__.py
|   |   fieldlist.json        # For AdObjects
|   |   params.json           # For Insight data
|   
└───processing
|   |   __init__.py
|   |   collectparams.py 
|   |   run.py
|
└───settings
|   |   __init__.py
|   |   settings.py 
|   
└───utils
|   |   __init__.py
|   |   getData.py 
|   |   s3FuncTools.py
|   |   version.py            # Will be removed in future
 
```

## Sample code...
```python
    from processing.run import RunProcess

    r = RunProcess()
    r.get_insights(saveto='data', data_limit=100)
    # File(s) will be stored in root/data directory
```


