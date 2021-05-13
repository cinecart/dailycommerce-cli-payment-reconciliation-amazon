import sys
import re
import typing
from datetime import datetime
from decimal import Decimal
from collections import Counter
from pathlib import Path
from shutil import copy2
from collections import defaultdict

from pprint import pprint

from .fuzzlogic import FuzzLogic

from .utility import readcsv, save_csv, file_with_suffix, file_with_prefix, create_directory


class PaymentDB:
    def __init__(self, source, options):       
        self._source = source
        self.options = options
        self._schema = ["Umsatz in Euro","Steuerschlüssel","Gegenkonto","Beleg1","Beleg2","Datum","Konto","Kost1","Kost2","Skonto in Euro","Buchungstext","Umsatzsteuer-ID","Zusatzart","Zusatzinformation"]
        self.fuzzy = FuzzLogic()
        account_file = options.get("account_list_file", None)
        try:
            account_list = readcsv(account_file)
        except Exception as err:
            print("[WARNING] Can't import account-list.csv")
            print(err)
            account_list = None
        self.account_list = account_list
        self.db = []


    def _process(self, payments, rules):
        """
        """      
        for record in payments:
            res = record["result"]
            for rule in rules:
                key = rule[0]
                transformer = rule[1]
                if callable(transformer):
                    args = [record[x] for x in rule[2:]]
                    res[key] = transformer(*args)
                else:
                    res[key] = transformer


    def _bank_process(self, konto):
        rules = [
            ["Umsatz in Euro", lambda x: x,                        "Betrag"           ],
            ["Beleg2",         lambda a, b: "{}, {}".format(a, b), "Name", "Zweck"    ],
            ["Datum",          lambda a: a,                        "Datum"            ],
            ["Konto",          konto,                                                 ],
            ["Buchungstext",   lambda a, b: "{} {}".format(a, b),  "Name", "Zweck"    ]
        ]
        return rules

    def _paypal_process(self, konto):
        rules = [
            ["Umsatz in Euro",    lambda x: x,                                "Brutto"                                          ],
            ["Beleg1",            lambda x: x,                                "Hinweis"                                         ],
            ["Beleg2",            lambda a, b: "{}, {}".format(a,b),          "Transaktionscode", "Zugehöriger Transaktionscode"],
            ["Datum",             lambda a, b: "{} {}".format(a,b),           "Datum", "Uhrzeit"                                ],
            ["Konto",             konto,                                                                                        ],
            ["Buchungstext",      lambda a, b, c: "{}, {}, {}".format(a,b,c), "Typ","Name","Empfänger E-Mail-Adresse"           ],
            ["Zusatzinformation", lambda a, b, c: "{}, {}, {}".format(a,b,c), "Auswirkung auf Guthaben", "Status", "Währung"    ]
        ]
        return rules

    def _first_run(self):
        schema = dict.fromkeys(self._schema)
        for path in self._source:
            print("Processing {}".format(path.name))
            name = path.name
            if re.match(r".*_result.csv$", name):
                print("skipping {}".format(name))
                continue
            try:
                db = readcsv(path)
            except Exception as ex:
                print("[ERROR] {}", format(ex)) 
                continue
            payment_source = self._get_source(db[0])
            konto = self._search_in_text(name, r"[^\d]*(\d{4,5})[^\d]*")
            for record in db:
                res = schema.copy()
                record["result"] = res
            if payment_source == "bank":
                self._process(db, self._bank_process(konto))
            elif payment_source == "paypal":
                db = sorted(db, key=self._sorting_key)
                self._process(db, self._paypal_process(konto))
            self.db.append({
                "file":name,
                "source": payment_source,
                "payments": db
            })
            print("done")

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

    def _get_source(self, payment):
        if "Brutto" in payment:
            return "paypal"
        elif "Betrag" in payment:
            return 'bank'
        return "unknown"

    def _second_run(self, pdfs:list):        
        cuelines = self.fuzzy.get_cuelines("cuelines.json")
        cnt = Counter()
        for pdf in pdfs:
            print()
            print("Assigning {}...".format(pdf.name))
            receipt_id = self._search_in_text(pdf.name, r"(PO-[\d-]+)[^\d-]*")
            if receipt_id:
                flag = False
                for part in self.db:
                    payments = part["payments"]
                    payment_source = part["source"]
                    if payment_source == "bank":
                        continue
                    for payment in payments:
                        beleg = str(payment["Hinweis"]).strip().lower()
                        if beleg == receipt_id.lower():
                            print("Match found for {}".format(receipt_id))
                            self.save_pdf(pdf)
                            flag = True
                            break
                    if flag:
                        break
                if not flag:
                    print("Couldn't find a match.")
                continue            
            lines, meta = self.fuzzy.get_document_data(pdf)
            candidate_lines = self.fuzzy.get_candidate_lines(cuelines, lines, meta)
            candidate_values = self._get_candidate_values(lines, candidate_lines)
            partial_matches = []
            for part in self.db:
                payments = part["payments"]
                payment_source = part["source"]
                if payment_source == "paypal":
                    continue
                for payment in payments:
                    matches = []
                    suplier = payment["Name"]                
                    res = self.fuzzy.extract(suplier, lines)
                    if res:
                        matches.append("Suplier")
                    amount = self._parse_decimal(payment["Betrag"], True)
                    purpose = payment["Zweck"].replace(" ", "").lower()
                    date = self._parse_date_time(payment["Datum"], "%d.%m.%y")    
                    if date in candidate_values["date"]:
                        matches.append("Date")
                    if amount in candidate_values["amount"]:
                        matches.append("Amount")
                    if list(filter(lambda x: x in purpose, candidate_values["invoice_nr"])):
                        matches.append("Invoice Number")
                    payment["result"]["matches"] = matches
                    if len(matches) >= 2:
                        partial_matches.append(payment)
            if not partial_matches:
                print("Couldn't find a good match.")
                self.save_pdf(pdf, assigned=False)
                continue
            sorted_matches = sorted(partial_matches,key=lambda x: len(x["result"]["matches"]), reverse=True)
            best_match = sorted_matches[0]
            match = (len(best_match["result"]["matches"]) / 4) * 100
            print("Best match: {}%".format(int(match)))
            print("Payment # {} matched with {}".format(best_match["#"], ", ".join(best_match["result"]["matches"])))
            date = self._parse_date_time(best_match["Datum"], "%d.%m.%y")
            sdate = date.strftime("%Y-%m")
            cnt[sdate] += 1
            receipt_id = self._generate_id("REC", date, cnt[sdate] )
            best_match["result"]["Beleg1"] = receipt_id
            self.save_pdf(pdf, receipt_id)

    # def _match_payment(self, candidate_values, name, amount, purpose, date)

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


    def _third_run(self):
        if not self.account_list:
            print("No account list file")
            return
        for part in self.db:
            payments = part["payments"]
            payment_source = part["source"]
            if payment_source == 'bank':
                tags = "tags_bank"
            elif payment_source == 'paypal':
                tags = "tags_paypal"
            else:
                continue    
            for payment in payments:
                if payment_source == 'bank':
                    name = payment["Name"]
                elif payment_source == 'paypal':
                    name = payment["Typ"]                
                res = self.fuzzy.get_best_match(self.account_list, name, tags)
                if res:
                    res = res["account"]["Gegenkonto"]
                payment["result"]["Gegenkonto"] = res


    def _get_candidate_values(self, lines, candidate_lines):
        candidate_lines = list(filter(lambda x: x["similarity"]>=90, candidate_lines))
        res = defaultdict(list)

        extraction_functions = self._get_extraction_function()
        for line in candidate_lines:
            param = line["keyline"]["parameter"]
            pos = line["position"]            
            text = lines[pos:pos+2]
            if param in extraction_functions:
                extractor = extraction_functions[param]
                value = extractor(text)
                if not value:
                    continue
                #print("{} : {}".format(param, value))
            else:
                pass
            res[param].append(value)
        return res

    def _extract_date(self, text:list):
        pat = re.compile(r"(\d{2,4})[\.-/](\d{2})[\.-/](\d{2,4})")        
        day, mon, year = 1, 1, 1900
        dt = None
        for line in text:
            match = pat.search(line)
            if match:
                if len(match.group(1)) == 2:
                    day, mon, year = match.group(1), match.group(2), match.group(3)                
                else:
                    day, mon, year = match.group(3), match.group(2), match.group(1)
                try:
                    dt = datetime(int(year), int(mon), int(day))
                except:
                    continue
                break
        return dt

    def _extract_amount(self, text:list):
        pat = re.compile(r"[^\d]*(-?(\d{1,5})[\.,](\d{2}))[^\d]*")        
        amount = None
        for line in text:
            match = pat.search(line)
            if match:
                amount = self._parse_decimal(match.group(1))
                break
        return amount

    def _extract_invoice_number(self, text:str):
        pat = re.compile(r"[^\w]*(([a-z]{1,2})?[\d-]{5,})[^\d]*")      
        invoice_nr = None
        for line in text:
            match = pat.search(line)
            if match:
                invoice_nr = match.group(1)
                break
        return invoice_nr

    def _get_extraction_function(self):
        return {
            "date"      : self._extract_date,
            "amount"    : self._extract_amount,
            "invoice_nr": self._extract_invoice_number,
        }

    def _generate_id(self, pref:str, dt:datetime, num:int) -> str:
        """Generate an receipt-id"""
        d = dt.strftime("%Y-%m")
        num_str = str(num)
        num_len = len(num_str)
        n = "0"*(6-num_len)+num_str if num_len <= int(6) else num_str
        id = "{}-{}{}".format(pref, d, n)
        return id

    def save_results(self):
        print()
        output_dir = self.options.get("output", None)
        if output_dir is None:
            source = self.options["payment_source"]
            source_path = Path(source)
            if source_path.is_file():
                output_dir = source_path.parent
            elif source_path.is_dir():
                output_dir = source_path.parent / "output"
        else:
            output_dir = Path(output_dir)
        if not output_dir.is_dir():
            create_directory(output_dir, self.options["debug"])
        for part in self.db:
            name = part["file"]
            # TODO: remove source
            output_file = file_with_suffix(name, '_result')
            output_path = output_dir / output_file
            results = []
            for record in part["payments"]:
                results.append(record["result"])            
            save_csv(output_path, results, self._schema)
        return True

        
    def _parse_decimal(self, raw_str: str, abs:bool=False) -> Decimal:
        """
        Parse string representing some price into a Decimal
        """      
        string_decimal = raw_str.replace(",", ".")
        #string_decimal = string_decimal.replace("-", "")   
        try: 
            result = Decimal(string_decimal)
            if abs and result < 0:
                result = 0 - result
        except:
            result = Decimal(0)
        return result

    def _parse_date_time(self, date_str:str, format:str) -> datetime:
        try:
            dt = datetime.strptime(date_str, format)
            return dt
        except:
            pass
        try:
            dt = datetime.fromisoformat(date_str)
        except:
            dt = datetime(1970,1,1,0,0,0,0)        
        return dt


