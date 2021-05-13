import re
import sys
import json

from pprint import pprint


import fitz
# MuPDF is a bit over-verbosed on warnings.
# We only want to handle exceptions
#fitz.TOOLS.mupdf_display_errors(False)
from rapidfuzz import fuzz, process



class FuzzLogic:
    def __init__(self):
        pass

    def get_cuelines(self, file):
        '''
        Returns all cuelines.
        '''
        with open(file, 'r') as f:
            data = json.load(f)
            lines = []

            for cue in data["cues"]:
                param = cue["parameter"]
                cuelines = cue["cuelines"]
                line = [{
                    'cueline': cueline,
                    'parameter': param
                } for cueline in cuelines]
                lines.extend(line)

        return lines


    def get_document_data(self, file, min_length=5):
        '''
        Basic page-to-page text extraction, cleansing, 
        and line splitting. Also returns number of pages
        '''
        try:
            meta = {}
            with fitz.open(file) as doc:
                meta['page_count'] = len(doc)
                lines = []

                for page in doc:
                    page_text = page.get_text('text')

                    # Cleansing
                    page_text = page_text.lower()
                    page_text = re.sub(r'[\*]', '', page_text)
                    for line_text in page_text.split('\n'):
                        line_text = line_text.replace('\t', ' ')
                        line_text = re.sub(r'\s+', ' ', line_text)

                        if len(line_text) < min_length:
                            # TODO CHECK IF NUMBERS
                            # too short lines causes noise in similarity
                            continue

                        lines.append(line_text.strip())
        except RuntimeError as e:
            # There are no specific exceptions on corrupted file
            # 
            raise RuntimeError(f'Corrupted PDF: {file}, MuPDF says: {str(e)}')
        return lines, meta


    def get_candidate_lines(self, keylines, lines, meta):
        """
        For each key line get the candidate
        lines from file lines with similarity metrics
        and line position

        :param threshold - minimum distance for a candidate to be considered,
            but we also want a stronger threshold for shorter words

        Output result only contains the keylines that have a candidate over threshold
        """
        results = []

        # pick the best candidate for each reference line
        for keyline in keylines:
            candidates = []

            for position, line in enumerate(lines):
                if len(line) < len(keyline['cueline']):
                    continue
                similarity = fuzz.token_set_ratio(keyline['cueline'], line, processor=True)
                candidates.append({
                    'line': line,
                    'similarity': similarity,
                    'position': position
                })
            if candidates:
                best_match = max(candidates, key=lambda c: c['similarity'])

                results.append({'keyline': keyline, **best_match})

        return results

    def get_best_match(self, queries, choice, tag_source):
        matches = []
        for account in queries:
            tags = account[tag_source]
            if not tags:
                continue
            tags = tags.split(",")
            for tag in tags:
                if len(tag) < 3:
                    continue
                similarity = fuzz.partial_ratio(tag, choice, processor=True)
                matches.append({
                    "account":account,
                    "similarity": similarity
                })
        best_match = max(matches, key=lambda m: m["similarity"])
        if best_match["similarity"] < 70:
            best_match = None
        return best_match



    def extract(self, query, choices, limit=5):
        if len(query) < 5:
            return None
        return process.extractOne(query, choices, scorer=fuzz.ratio,score_cutoff=55)
        #return process.extract(query, choices, limit=limit)