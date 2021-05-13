#!/usr/bin/env python3

import sys
import time
from pathlib import Path
from argparse import Namespace, ArgumentParser

from modules.payments import PaymentDB



def load_config(args:'Namespace') -> dict:
    """
    Loads a configuration mapping object with contents of a given file.

    :param args: Commad-line arguments
    :returns: mapping with configuration parameter values
    """
    data = {}
    config_path = Path(args.config)
    if not config_path.is_file():
        sys.exit("[Error] Couldn't find the config file: {}".format(config_path))
        return dict()      
    suf = config_path.suffix
    if suf == ".json":
        import json                     
        data = json.loads(config_path.read_text(encoding="utf-8"))
    else:
        sys.exit("Invalid file type {}. Supported file types - .json".format(config_path.name))
    return data


def all_files_with_ext(source_name: str, ext:str) -> list:
    """Finds all files of desired type in the given directory"""
    source_path = Path(source_name)
    if source_path.is_file():
        if source_path.suffix == ext:
            return [source_path]
        else:
            message = "[Error] Invalid file type %s" % source_name
            raise Exception(message)
    elif source_path.is_dir():
        all_files = list(source_path.glob("**/*{}".format(ext)))
        if len(all_files) == 0:
            print("Folder %s is empty" % source_name)
        return all_files
    else:
        message = "[Error] No such file of directory: %s" % source_name
        raise FileNotFoundError(message)
        

def parse_comand_line(dir_path: 'Path') -> 'Namespace':
    """
    Command-line interface for the application
    """
    parser: ArgumentParser = ArgumentParser(
        prog="unpack", description=""
    )

    parser.add_argument(
        "-D",
        "--debug",
        dest="debug",
        action="store_true",
        default=False,
        help="Debug",
    )
    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        default=dir_path / "config.json",
        help="Path to configuration file",
    )
    parser.add_argument(
        "-r",
        "--receipts",
        dest="receipt_dir",
        help="Path to the directory with receipts."
        "Overrides the value from your configuration file.",
    )
    parser.add_argument(
        "-p",
        "--payments",
        dest="payment_source",
        help="Path to the payment source directory or file."
        "Overrides the value from your configuration file.",
    )
    parser.add_argument(
        "-a",
        "--accounts",
        dest="account_list_file",
        help="Path to the account-list file."
        "Overrides the value from your configuration file.",
    )
    
    args = parser.parse_args()
    return args


def main():
    tt = time.time()
    parent_directory = Path(__file__).resolve().parent
    args = parse_comand_line(parent_directory)
    config = load_config(args)
    args_list = [(key, val) for key, val in vars(args).items() if val != None]
    config.update(args_list)
    
    payment_source = config["payment_source"]
    source_files = all_files_with_ext(payment_source, ".csv")
    payment_db = PaymentDB(source_files, config)
    # First LOOP
    payment_db._first_run()
    # Second LOOOP
    pdf_dir = config["receipt_dir"]
    source_pdfs = all_files_with_ext(pdf_dir,".pdf")
    payment_db._second_run(source_pdfs)
    # # Third LOOP
    payment_db._third_run()

    if payment_db.save_results():
        print("All done in {} seconds".format(time.time() - tt))
        

if __name__ == "__main__":
    main()

    