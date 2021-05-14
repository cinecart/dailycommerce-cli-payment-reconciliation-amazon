from os import pipe
import sys
import re
import typing
import json
from datetime import datetime
from decimal import Decimal
from collections import Counter
from pathlib import Path
from shutil import copy2
from collections import defaultdict

from pprint import pprint

#from .fuzzlogic import FuzzLogic

from .utility import printProgressBar, readcsv, save_csv, file_with_suffix, file_with_prefix, create_directory


class PaymentDB:
    payment_schema = ["date/time", "settlement id", "type", "order id", "sku", "description", "quantity", "marketplace",
    "fulfilment", "order city", "order state", "order postal", "tax collection model", "product sales", "product sales tax",
    "postage credits", "shipping credits tax", "gift wrap credits", "giftwrap credits tax", "promotional rebates",
    "promotional rebates tax", "marketplace withheld tax", "selling fees", "fba fees", "other transaction fees", "other", "total"]

    fees_schema = ["Umsatz in Euro", "Steuerschlüssel", "Gegenkonto", "Beleg1", "Beleg2", "Datum", "Konto", "Kost1", "Kost2",
    "Skonto in Euro", "Buchungstext", "Umsatzsteuer-ID", "Zusatzart", "Zusatzinformation"]

    result_schema = ["Umsatz in Euro", "Steuerschlüssel", "Gegenkonto", "Beleg1", "Beleg2", "Datum", "Konto", "Kost1", "Kost2",
    "Skonto in Euro", "Buchungstext", "Umsatzsteuer-ID", "Zusatzart", "Zusatzinformation"]

    receipt_schema = ["Umsatz in Euro", "Steuerschlüssel", "Gegenkonto", "Beleg1", "Beleg2", "Datum", "Konto", "Kost1", "Kost2",
    "Skonto in Euro", "Buchungstext", "Umsatzsteuer-ID", "Zusatzart", "Zusatzinformation"]

    skip_lines = 8

    def __init__(self, source, options):       
        self._source = source
        self.options = options
        self._schema = ["Umsatz in Euro","Steuerschlüssel","Gegenkonto","Beleg1","Beleg2","Datum","Konto","Kost1","Kost2",
        "Skonto in Euro","Buchungstext","Umsatzsteuer-ID","Zusatzart","Zusatzinformation"]
        self.db = []
        self.receipts = self.load_receipts(options["receipt_source"])
        self.cues = self.get_cuelines('dailycommerce-cli-payment-reconciliation-amazon-cuelines.json')


    def process(self):
        result = []
        _schema = dict.fromkeys(self.result_schema)
        fee_schema = dict.fromkeys(self.fees_schema)
        fees_result = []
        total_sales = 0
        total_fees = 0
        total_payouts = 0
        total_reimbursements = 0

        for store in self.db:
            lang = store['lang']
            payments = store['payments']
            account = store['account']
            for payment in payments:
                res = _schema.copy()
                order = payment["order id"]
                summa = self._parse_decimal(payment["product sales"])
                shiping = self._parse_decimal(payment["postage credits"])
                description = payment["description"]
                fees = self._parse_decimal(payment["selling fees"])
                payment_type = payment["type"]
                dt = self._parse_date_time(payment["date/time"])
                gegenkonto = None
                beleg1 = None
                total = self._parse_decimal(payment["total"])
             
                if payment_type in self.cues["Payouts"]:
                    # transfer
                    gegenkonto = self.options["account_bank"]
                    beleg1 = payment_type
                    total_payouts = total_payouts + total               
                elif payment_type in self.cues["Fees"]:
                    gegenkonto = self.options["amazon_account"]
                    total_fees = total_fees + total
                else :
                    gegenkonto = self.options["sales_account"]
                    total = summa + shiping
                    if payment_type in self.cues["Refund"]:
                        total_reimbursements = total_reimbursements + total
                    else:
                        total_sales = total_sales + total                    

                total_receipts = None
                if order:
                    receipts = self.find_receipt(order)
                    if receipts:
                        total_receipts = 0
                        beleg1 = receipts[0]["Beleg1"]
                        for receipt in receipts:
                            sale = self._parse_decimal(receipt["Umsatz in Euro"])
                            total_receipts = total_receipts + sale
                        if total_receipts != total:
                            total_receipts = "#DIFF!" + " " + str(total_receipts)
                    else:
                        total_receipts = "#UNKNOWN!"
                    
                    # FEES
                    fee = fee_schema.copy()
                    fee["Umsatz in Euro"] = self._decimal_tostring(fees)                
                    fee["Gegenkonto"] = self.options["amazon_account"]                                
                    fee["Beleg1"] = order                                        
                    fee["Beleg2"] = order
                    fee["Datum"] = dt.strftime("%d.%m.%Y %H:%M:%S")                 
                    fee["Konto"] = account                                    
                    fee["Buchungstext"] = description 
                    fee["Zusatzinformation"] = payment_type
                    fees_result.append(fee)
    
                res["Umsatz in Euro"] = self._decimal_tostring(total)           # "product sales" + "postage credits" / "total" for transfers
                res["Steuerschlüssel"] = self._decimal_tostring(total_receipts) # sum of receipts["Umsatz in Euro"]
                res["Gegenkonto"] = gegenkonto                                  # options["sales_account"] / options["account_bank"] for transfers
                res["Beleg1"] = beleg1                                          # recipts["Beleg1"] / "type" for transfers
                res["Beleg2"] = order if order else description
                res["Datum"] = dt.strftime("%d.%m.%Y %H:%M:%S")                 # "date/time" reformatted 
                res["Konto"] = account                                          # account 
                res["Buchungstext"] = description if order else payment_type    # "description" / "type" for transfers
                res["Zusatzinformation"] = payment_type                         # "type"

                result.append(res)

        assigned = list(filter(lambda x: x["assigned"], self.receipts))
        unassigned = list(filter(lambda x: not x["assigned"], self.receipts))
        print()
        print("Assigned {} receipts".format(len(assigned)))
        print("Unassigned receipts left: {}".format(len(unassigned)))

        report =[
            {"text":"Total Sales", "amount": self._decimal_tostring(total_sales)},
            {"text":"Total Reimboursements", "amount": self._decimal_tostring(total_reimbursements)},
            {"text":"Total Paypouts", "amount": self._decimal_tostring(total_payouts)},
            {"text":"Total Fees", "amount": self._decimal_tostring(total_fees)}
        ]
        report_str = [";Amount"]
        print("{:<25}{:>10}".format("", "AMOUNT"))
        for entry in report:
            print("{:<25}{:>10}".format(entry["text"], entry["amount"]))
            report_str.append(entry["text"] + ";" + entry["amount"])
        report_str = "\n".join(report_str)
        print()
        print("Saving results...",)
        result_file = self.options.get("result_payments_assigned", "result-payments-assigned.csv")
        self.save_results(result, result_file, self.result_schema)
        fees_file = self.options.get("result_amazon-fees", "result-amazon-fees.csv")
        self.save_results(fees_result, fees_file, self.fees_schema)
        if unassigned:
            unassigned_file = "result-receipts-left.csv"
            self.save_results(unassigned, unassigned_file, self.receipt_schema)
        self.save_results(report_str, "result-report.csv", None, istext=True)
        print("done!")

        return True


    def find_receipt(self, order):
        result = []
        id = None
        for receipt in self.receipts:
            if order == receipt["Zusatzinformation"]:
                id = receipt["Beleg1"]
                break
        result = list(filter(lambda x: x["Beleg1"] == id, self.receipts))
        for receipt in result:
            receipt["assigned"] = True

        return result


    def load_payments(self):
        options = self.options
        for path in self._source:
            name = path.name
            print("Loading {}...".format(name), end = "")
            if re.match(r"result.*\.csv$", name):
                print("skipping {}".format(name))
                continue
            try:
                db = readcsv(path, fieldset=self.payment_schema, skip_row=self.skip_lines)
            except Exception as ex:
                print("[ERROR] {}", format(ex)) 
                continue
            lang = self._search_in_text(name, r"(?<=[\-_])([A-Z]{2})[_\-\.]")
            # print(lang)
            # print("Records: {}".format(len(db)))

            account_key = "account_" + lang.upper()
            account = options[account_key]
            
            self.db.append({
                "file":name,
                "lang":lang,
                "payments": db,
                "account": account
            })
            print("...done!")
            

    def load_receipts(self, path):
        print("Loading {}...".format(path), end = "")
        db = readcsv(path)
        for receipt in db:
            receipt["assigned"] = False
        print("...done!")
        return db


    def get_cuelines(self, file):
        '''
        Returns all cuelines.
        '''
        try:
            with open(file, 'r', encoding="utf-8") as f:
                data = json.load(f)
                lines = dict()
                for cue in data["cues"]:
                    param = cue["parameter"]
                    cuelines = cue["cuelines"]
                    lines[param] = cuelines
        except json.decoder.JSONDecodeError as err:
            sys.exit("Cuelines.json is not proper JSON. Hint: {}".format(err))
        except Exception as ee:
            sys.exit(ee)
            
        return lines



    def _sorting_key(self, x):
        date_str = x["Datum"] + " " + x["Uhrzeit"]
        dt = self._parse_date_time(date_str, "%d.%m.%Y %H:%M:%S")
        return dt
       

    def _search_in_text(self, text:str, pattern:str):
        """
        pattern must contain 1 (group) 
        """
        pat = re.compile(pattern,flags=re.IGNORECASE)
        match = pat.search(text)
        if match:
            return match.group(1)
        return None


    def save_pdf(self, path, receipt_id=None, assigned=True):
        """Copies assigned pdf file to assigned directory"""
        output_dir = self.options.get("output", None)
        if not output_dir:
            output_dir = (Path(self.options["receipt_dir"]).parent) / "results"
        else:
            output_dir = Path(output_dir)
        if not output_dir.is_dir():
            create_directory(output_dir, self.options["debug"])
        if assigned:
            output_dir = output_dir / "receipts_assigned"
        else:
            output_dir = output_dir / "receipts_unassigned"
        if not output_dir.is_dir():
            create_directory(output_dir, self.options["debug"])
        input_file = path.resolve()
        if receipt_id:
            output_file = file_with_prefix(path, receipt_id).resolve()
        else:
            output_file = path
        dst = output_dir / output_file.name
        copy2(input_file, dst)


    def save_results(self, results, file, fieldnames, istext=False):
        output_dir = self.options.get("results", None)
        if output_dir is None:
            source = self.options["payment_source"]
            source_path = Path(source)
            if source_path.is_file():
                output_dir = source_path.parent
            elif source_path.is_dir():
                output_dir = source_path.parent / "results"
        else:
            output_dir = Path(output_dir)
        if not output_dir.is_dir():
            create_directory(output_dir, self.options["debug"])
            # TODO: remove source
        result_file = file
        output_path = output_dir / result_file
        if istext:
            output_path.write_text(results)
        else: 
            save_csv(output_path, results, fieldnames)
        return True


    def _decimal_tostring(self, dec:'Decimal') -> str:
        if not isinstance(dec, Decimal):
            return dec
        res = str(dec)
        res = res.replace(".", ",")
        return res

        
    def _parse_decimal(self, raw_str: str, abs:bool=False) -> Decimal:
        """
        Parse string representing some price into a Decimal
        """      
        regex = re.compile(r'^(?P<sign>-?)(?P<tous>\d{1,3})[^\d](?P<hun>\d{3}),(?P<dec>\d{2})')
        m = regex.search(raw_str)
        if m:
            string_decimal = m.group("sign") + m.group("tous") + m.group("hun") + "." + m.group("dec")
        else:
            string_decimal = raw_str.replace(",", ".")
        try: 
            result = Decimal(string_decimal)
            if abs and result < 0:
                result = 0 - result
        except Exception as err:
            print("Can't parse number {}. Unknown format".format(raw_str))
            result = Decimal(0)
        return result

    months = {"jan":"01","janv":"01","genn":"01","gen":"01","ene":"01",
            "febr":"02","feb":"02","févr":"02","fév":"02","fev":"02",
            "febbr":"02","febb":"02","maar":"03","maa":"03","mar":"03",
            "mars":"03","märz":"03","mär":"03","marz":"03","apr":"04",
            "avr":"04","mei":"05","may":"05","mai":"05","maj":"05","magg":"05",
            "mag":"05","mayo":"05","jun":"06","juin":"06","giug":"06",
            "giu":"06","jui":"06","jul":"07","juil":"07","jui":"07","lugl":"07",
            "lug":"07","aug":"08","août":"08","aoû":"08","agos":"08","ago":"08",
            "sept":"09","sep":"09","sett":"09","set":"09","okt":"10",
            "otto":"10","ott":"10","nov":"11","dec":"12","déc":"12","dez":"12",
            "dic":"12"}

    def _parse_date_time(self, date_str:str) -> datetime:
        # 9 ene. 2018 12:33:58 UTC
        # 03.01.2018 09:41:31 UTC
        regex1 = re.compile(r"^(?P<day>\d{1,2})\s*(?P<mon>[^\d\s\.\-_]{2,5})\.?\s*(?P<year>\d{2,4})\s*(?P<hour>\d{1,2}):(?P<min>\d{1,2}):(?P<sec>\d{1,2})(\s[A-Z]+)$")
        regex2 = re.compile(r"^(?P<day>\d{1,2})\.(?P<mon>\d{1,2})\.(?P<year>\d{2,4})\s+(?P<hour>\d{1,2}):(?P<min>\d{1,2}):(?P<sec>\d{1,2})(\s[A-Z]+)$")
        m = regex1.match(date_str)
        try:
            if m:
                month = int(self.months[m.group('mon').lower()])
            else:
                m = regex2.match(date_str)
                month = int(m.group('mon'))
            year = int(m.group('year'))
            day = int(m.group('day'))
            hour = int(m.group('hour'))
            min = int(m.group('min'))
            sec = int(m.group('sec'))
            dt = datetime(year, month, day, hour, min, sec)
            return dt
        except Exception as err:
            print(err)
            print(date_str)
            print(m)
            pass
        try:
            dt = datetime.fromisoformat(date_str)
        except:
            dt = datetime(1970,1,1,0,0,0,0)        
        return dt


