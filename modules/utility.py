import csv
from pathlib import Path
from functools import lru_cache
from difflib import SequenceMatcher


def readcsv(p:'Path', fieldset:list=None, skip_row=0) -> list:
    """
    Load contents of a csv file

    :param p: Path to the csv file
    :param fieldset: 
    :returns: Contents of the csv file
    """
    pp = p
    # Check if p is not instance of Path class
    if isinstance(p, str):
        pp = Path(p)
    # Check if file exists. If not exit Python
    if not pp.is_file():
        raise Exception('File {} does not exist'.format(p))
    # Check if file is csv. If not exit Python
    suf = pp.suffix
    if suf != ".csv":
        raise Exception("Invalid file type {}. Supply a csv file".format(p))
    db = []
    # Read csv file 
    line = ""
    delim = ","
    with open(pp, encoding="utf-8") as f:
        line = f.readlines()[skip_row:skip_row + 1]
    if line:
        num1 = line[0].count(";")
        num2 = line[0].count(",")
        delim = ";" if num1 > num2 else ","
    else:
        raise Exception("File {} is empty".format(p))

    count = 0
    with pp.open(encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file, fieldnames=fieldset, delimiter=delim)
        try:
            for row in csv_reader:
                if count < skip_row:
                    count += 1
                    continue
                db.append(row)
        except csv.Error as e:
            raise Exception('File {}, line {}: {}'.format(str(p), csv_reader.line_num, e))
    if not db:
        raise Exception("File {} is empty".format(p))
    return db


def save_csv(target:str, data:list, fieldnames:list) -> None:
        """
        Write list into target.csv file
        :param target: Target file Path or string
        :returns: None
        """
        target_path = target
        if isinstance(target, str):
            target_path = Path(target)
        # Write table into a file
        print("Saving results into %s..." % target)
        if not data:
            print("[WARNING] Resulting file is empty")
            data = []
        with target_path.open('w', encoding='utf-8', newline='') as target_file:
            writer = csv.DictWriter(target_file, fieldnames=fieldnames, extrasaction='ignore', 
                delimiter=';', quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            writer.writerows(data)

def create_directory(output_dir_path:Path, debug:bool):
    """
    Attempt to create a directory
    :param
        output_dir_path: Path object
        debug: show additional messages

    :raises
        FileNotFoundError
    """
    if debug:
        print("No such directory %s. Attempting to create" % output_dir_path)
    try:  
        output_dir_path.mkdir()
    except:
        message = "Output directory %s is missing. Attempt to create one FAILED" % output_dir_path
        raise FileNotFoundError(message)

@lru_cache()
def find_similarity(a:str, b:str) -> int:
    return int(SequenceMatcher(None, a, b).ratio() * 100)


def file_with_suffix(f:str, s:str) -> 'Path':
    target = f
    if isinstance(target, str):
        target = Path(f)
    target_stem = target.stem
    target_parent = target.parent
    target_suf = target.suffix
    if target_stem[-7:] == "_source":
        target_stem = target_stem[:-7]
    new_stem = target_stem + s
    return (target_parent / new_stem).with_suffix(target_suf)

def file_with_prefix(f:str, p:str) -> 'Path':
    target = f
    if isinstance(target, str):
        target = Path(f)
    target_stem = target.stem
    target_parent = target.parent
    target_suf = target.suffix
    new_stem = "-".join((p, target_stem))
    return (target_parent / new_stem).with_suffix(target_suf)


def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    :params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()
