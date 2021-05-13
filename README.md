# Payment Reconciliation

## Table of Contents
  - [Requirements](#requirements)
  - [Configuration](#configuration)
  - [Usage](#usage)
  
## Requirements   

1. Python 3.7+   
2. Third-party python packages (see `requirements.txt`)   

### PyMuPdf  

Library for reading pdf files   

### rapidfuzz  

Library for calculating Levenshtein distance between the strings (finding similarity)  

To install them both  

    pip3 install requirements.txt

## Configuration


### Configuration File 

    {
        "receipt_dir":"receipts_source",
        "assigned_receipt_dir":"receipts_assigned",
        "payment_source":"payment_source",
        "account_list_file":"account-list.csv",
        "output":"output"
    }

"receipt_dir" - Path to the directory with receipts.    

"assigned_receipt_dir" - Path to the directory with assigned receipts.  

"payment_source" - Path to the payment source directory or file.       

"account_list_file" - Path to the account-list file.   

"output" - path to the output directory.    



### Cuelines File

Has folowing structure

    {
        "parameter":"amount",
        "cuelines":[
            "Zu zahlender Betrag",
            "Rechnungsbetrag"
        ]
    }
 
"parameter" - amout, date or invoice number

"cuelines" - list of the cue phrases, words that can be on the same line as the coresponding parameter. (shouldn't be shorter than 5 characters)


## Usage and CLI commands


    python3 main.py


### Optional arguments


#### -r or --receipts  
Path to the directory with receipts.  
        Overrides the value from your configuration file.  

    python3 main.py -r /path/to/receipts_source   

#### -p or --payments  
Path to the payment source file.
        Overrides the value from your configuration file.   

    python3 main.py -p /path/to/payment_source.csv

#### -a or --accounts  
Path to the account-list file.
        Overrides the value from your configuration file.

    python3 main.py -a "account-list.csv"




