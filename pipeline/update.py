#!/usr/bin/env python3
"""Incremental updater for the Core Remedial Action Map (JAO Core CCR, preventive & curative RAs).

On GitHub Actions the workflow runs:  python3 pipeline/update.py --site . --fetch

Ways to get new data:
  1. --raw-chunks FILE [FILE ...]  : tool-result files written when the browser snippet's jaoDump.chunk(i) output was saved
     --raw FILE.json.gz            : a dump produced by the same snippet, already decoded
  2. --fetch                        : call the JAO API directly (works on servers that can reach publicationtool.jao.eu)

The site directory must contain catalog.json and the month files data/YYYY-MM.json (all of them: usage
statistics are recomputed over the full history). Changed files are rewritten in place and a JSON summary
is printed, including the list of files to republish.
"""
import argparse, base64, collections, datetime, gzip, hashlib, io, json, os, re, sys, time, unicodedata, urllib.request, zoneinfo


def norm(s):
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    return re.sub(r'[^a-z0-9]', '', s.lower().replace('ß', 'ss'))

# token (normalised) -> site id. Site ids are defined in SITES (coordinates filled later).
ALIASES = {
    # 50Hertz
    'roehrsdorf': 'DE-50H-ROEHRSDORF', 'rohrsdorf': 'DE-50H-ROEHRSDORF',
    'vierraden': 'DE-50H-VIERRADEN',
    'hamburgost': 'DE-50H-HAMBURGOST',
    'guestrow': 'DE-50H-GUESTROW', 'gustrow': 'DE-50H-GUESTROW',
    # Amprion
    'gronau': 'DE-AMP-GRONAU', 'oberzier': 'DE-AMP-OBERZIER',
    # APG
    'tapst': 'AT-APG-TAUERN', 'tauern': 'AT-APG-TAUERN',
    'ehpst': 'AT-APG-ERNSTHOFEN', 'ernsthofen': 'AT-APG-ERNSTHOFEN',
    'tepst': 'AT-APG-TERNITZ', 'ternitz': 'AT-APG-TERNITZ',
    'yfpst': 'AT-APG-YBBSFELD', 'ybbsfeld': 'AT-APG-YBBSFELD',
    'spkpl1': 'AT-APG-STPETER', 'stpeter': 'AT-APG-STPETER', 'stpetertr3': 'AT-APG-STPETER',
    'lipst': 'AT-APG-LIENZ', 'nrpst': 'AT-APG-NAUDERS',
    # CEPS
    'chrd': 'CZ-CEPS-HRADEC', 'hradec': 'CZ-CEPS-HRADEC',
    'nos': 'CZ-CEPS-NOSOVICE', 'nosovice': 'CZ-CEPS-NOSOVICE',
    'cprn': 'CZ-CEPS-PROSENICE', 'prosenice': 'CZ-CEPS-PROSENICE',
    'ccst': 'CZ-CEPS-CECHYSTRED', 'cstred': 'CZ-CEPS-CECHYSTRED',
    'clis': 'CZ-CEPS-LISKOVEC', 'liskovec': 'CZ-CEPS-LISKOVEC',
    'csok': 'CZ-CEPS-SOKOLNICE', 'sokolnice': 'CZ-CEPS-SOKOLNICE',
    # ELES
    'podlog': 'SI-ELES-PODLOG', 'divaca': 'SI-ELES-DIVACA',
    # Elia
    'vanyk': 'BE-ELIA-VANEYCK', 'vaneyck': 'BE-ELIA-VANEYCK',
    'zandv': 'BE-ELIA-ZANDVLIET', 'zandvliet': 'BE-ELIA-ZANDVLIET',
    'avlgm': 'BE-ELIA-AVELGEM', 'avelgem': 'BE-ELIA-AVELGEM',
    'courc': 'BE-ELIA-COURCELLES', 'courcelles': 'BE-ELIA-COURCELLES',
    'horta': 'BE-ELIA-HORTA',
    'doel': 'BE-ELIA-DOEL',
    'brueg': 'BE-ELIA-BRUEGEL', 'bruegel': 'BE-ELIA-BRUEGEL',
    'auban': 'BE-ELIA-AUBANGE', 'aubange': 'BE-ELIA-AUBANGE',
    'brume': 'BE-ELIA-BRUME',
    'monceau': 'BE-ELIA-MONCEAU',
    'gramme': 'BE-ELIA-GRAMME', 'mercator': 'BE-ELIA-MERCATOR', 'lixhe': 'BE-ELIA-LIXHE', 'achene': 'BE-ELIA-ACHENE',
    # MAVIR
    'gyor': 'HU-MAVIR-GYOR', 'wien': 'AT-APG-WIENSO', 'neus': 'AT-APG-NEUSIEDL', 'god': 'HU-MAVIR-GOD', 'gony': 'HU-MAVIR-GONYU', 'gonyu': 'HU-MAVIR-GONYU',
    # PSE
    'mikulowa': 'PL-PSE-MIKULOWA', 'joachimow': 'PL-PSE-JOACHIMOW', 'grudziadz': 'PL-PSE-GRUDZIADZ',
    # RTE
    'avelin': 'FR-RTE-AVELIN', 'vigy': 'FR-RTE-VIGY', 'lonny': 'FR-RTE-LONNY', 'mastaing': 'FR-RTE-MASTAING',
    'mery': 'FR-RTE-MERY', 'bezaumont': 'FR-RTE-BEZAUMONT', 'chevalet': 'FR-RTE-CHEVALET', 'gavrelle': 'FR-RTE-GAVRELLE',
    'moulaine': 'FR-RTE-MOULAINE', 'moulaine225': 'FR-RTE-MOULAINE', 'mazures': 'FR-RTE-MAZURES', 'capelle': 'FR-RTE-CAPELLE',
    'chooz': 'FR-RTE-CHOOZ', 'muhlbach': 'FR-RTE-MUHLBACH', 'scheer': 'FR-RTE-SCHEER',
    # SEPS
    'sucany': 'SK-SEPS-SUCANY', 'varin': 'SK-SEPS-VARIN', 'gabcikovo': 'SK-SEPS-GABCIKOVO',
    'pbiskupice': 'SK-SEPS-PBISKUPICE', 'vola': 'SK-SEPS-VOLA', 'vkapusany': 'SK-SEPS-VKAPUSANY',
    # Transelectrica
    'arad': 'RO-TEL-ARAD', 'urechesti': 'RO-TEL-URECHESTI', 'mintia': 'RO-TEL-MINTIA',
    # TenneT DE
    'diele': 'DE-TTG-DIELE', 'grafenrheinfeld': 'DE-TTG-GRAFENRHEINFELD', 'bergrheinfeld': 'DE-TTG-BERGRHEINFELD',
    'doerpenwest': 'DE-TTG-DOERPENWEST', 'doerpen': 'DE-TTG-DOERPENWEST',
    'pleinting': 'DE-TTG-PLEINTING', 'sittling': 'DE-TTG-SITTLING',
    'krempermarsch': 'DE-TTG-KREMPERMARSCH', 'wuergau': 'DE-TTG-WUERGAU',
    # TenneT NL
    'meeden': 'NL-TTB-MEEDEN', 'diemen': 'NL-TTB-DIEMEN', 'rilland': 'NL-TTB-RILLAND', 'maasbracht': 'NL-TTB-MAASBRACHT',
    'lelystad': 'NL-TTB-LELYSTAD', 'dodewaard': 'NL-TTB-DODEWAARD', 'eindhoven': 'NL-TTB-EINDHOVEN', 'zwolle': 'NL-TTB-ZWOLLE',
    'ens': 'NL-TTB-ENS', 'hengelo': 'NL-TTB-HENGELO', 'geertruidenberg': 'NL-TTB-GEERTRUIDENBERG',
    'doetinchem': 'NL-TTB-DOETINCHEM', 'krimpen': 'NL-TTB-KRIMPEN', 'borssele': 'NL-TTB-BORSSELE',
    'maasvlakte': 'NL-TTB-MAASVLAKTE', 'beverwijk': 'NL-TTB-BEVERWIJK', 'oostzaan': 'NL-TTB-OOSTZAAN',
    # HOPS
    'ernestinovo': 'HR-HOPS-ERNESTINOVO', 'ernestinovo1': 'HR-HOPS-ERNESTINOVO', 'konjsko': 'HR-HOPS-KONJSKO', 'konjsko2': 'HR-HOPS-KONJSKO',
    # Transelectrica cross-border partners
    'tantareni': 'RO-TEL-TANTARENI', 'kozlodui': 'BG-ESO-KOZLODUY', 'kozloduy': 'BG-ESO-KOZLODUY', 'resita': 'RO-TEL-RESITA', 'pancevo': 'RS-EMS-PANCEVO',
    # TransnetBW
    'buers': 'AT-TBW-BUERS', 'kuehmoos': 'DE-TBW-KUEHMOOS', 'kuehmoss': 'DE-TBW-KUEHMOOS',
    'dellmensingen': 'DE-TBW-DELLMENSINGEN', 'eichstetten': 'DE-TBW-EICHSTETTEN',
}

TSO_NAMES = {
    '50hertz': '50Hertz', 'amprion': 'Amprion', 'apg': 'APG', 'ceps': 'ČEPS', 'eles': 'ELES', 'elia': 'Elia',
    'hops': 'HOPS', 'mavir': 'MAVIR', 'pse': 'PSE', 'rte': 'RTE', 'seps': 'SEPS', 'transelectrica': 'Transelectrica',
    'tel': 'Transelectrica', 'tennetgmbh': 'TenneT DE', 'tennetbv': 'TenneT NL', 'tennetnl': 'TenneT NL',
    'transnetbw': 'TransnetBW', 'creos': 'Creos',
}
SITE_TSO = {'HR-HOPS': 'HOPS', 'BG-ESO': 'ESO', 'RS-EMS': 'EMS', 'DE-50H': '50Hertz', 'DE-AMP': 'Amprion', 'AT-APG': 'APG', 'CZ-CEPS': 'ČEPS', 'SI-ELES': 'ELES',
            'BE-ELIA': 'Elia', 'HU-MAVIR': 'MAVIR', 'PL-PSE': 'PSE', 'FR-RTE': 'RTE', 'SK-SEPS': 'SEPS',
            'RO-TEL': 'Transelectrica', 'DE-TTG': 'TenneT DE', 'NL-TTB': 'TenneT NL', 'AT-TBW': 'TransnetBW',
            'DE-TBW': 'TransnetBW'}

def tso_display(t):
    return TSO_NAMES.get(norm(t or ''), t or '')

def classify(name):
    n = name.strip()
    u = n.upper()
    if u.startswith('PST'):
        return 'PST', 'Phase-shifting transformer tap'
    if u.startswith('AT_') or u.startswith('AT '):
        return 'PST', 'Phase-shifting autotransformer tap'
    if u.startswith('TR_') or u.startswith('TR '):
        return 'TAP', 'Transformer tap change'
    if u.startswith('SPS'):
        return 'SPS', 'Special protection scheme'
    if u.startswith('HVDC'):
        return 'HVDC', 'HVDC set-point'
    if u.startswith('TOP'):
        if re.search(r'TOP_CLOSE', u):
            return 'CLOSE', 'Switching in an element'
        if re.search(r'TOP_1N', u):
            return 'MERGE', 'Bus-bar coupling (1 node)'
        if re.search(r'TOP_OPEN', u):
            return 'OPEN', 'Opening a coupler or line'
        return 'SPLIT', 'Bus-bar split'
    return 'OTHER', 'Other remedial action'

def sites_in(name):
    """Return list of site ids found in the RA name, in order, without duplicates."""
    body = re.sub(r'^(PST|AT|TR|TOP|SPS)[ _]+', '', name, flags=re.I)
    body = re.sub(r'^(OPEN|CLOSE|COMPLEX|2NH|2NV|2N|1N)[ _]+', '', body, flags=re.I)
    body = re.sub(r'^(2N|1N)[ _]+', '', body, flags=re.I)
    toks = [t for t in re.split(r'[_\s/\-]+', body) if t]
    found = []
    i = 0
    while i < len(toks):
        hit = None
        for span in (3, 2, 1):
            if i + span <= len(toks):
                key = norm(''.join(toks[i:i + span]))
                # allow 'P.Biskupice' -> 'pbiskupice', 'V.Kapusany' -> 'vkapusany'
                if key in ALIASES:
                    hit = (ALIASES[key], span)
                    break
        if hit:
            if hit[0] not in found:
                found.append(hit[0])
            i += hit[1]
        else:
            i += 1
    return found

# substation coordinates (OpenStreetMap / JAO Static Grid Model), keyed by site id
SITE_COORD = {
"DE-50H-ROEHRSDORF": {
"id": "DE-50H-ROEHRSDORF",
"name": "Röhrsdorf",
"cc": "DE",
"tso": "50Hertz",
"lat": 50.85866,
"lon": 12.81512,
"src": "OpenStreetMap way 23117075"
},
"DE-50H-VIERRADEN": {
"id": "DE-50H-VIERRADEN",
"name": "Vierraden",
"cc": "DE",
"tso": "50Hertz",
"lat": 53.09805,
"lon": 14.26865,
"src": "OpenStreetMap way 170123812"
},
"DE-50H-HAMBURGOST": {
"id": "DE-50H-HAMBURGOST",
"name": "Hamburg/Ost",
"cc": "DE",
"tso": "50Hertz",
"lat": 53.55538,
"lon": 10.15948,
"src": "OpenStreetMap way 23583867"
},
"DE-50H-GUESTROW": {
"id": "DE-50H-GUESTROW",
"name": "Güstrow",
"cc": "DE",
"tso": "50Hertz",
"lat": 53.81576,
"lon": 12.18825,
"src": "OpenStreetMap way 32062544"
},
"DE-AMP-GRONAU": {
"id": "DE-AMP-GRONAU",
"name": "Gronau",
"cc": "DE",
"tso": "Amprion",
"lat": 52.20155,
"lon": 7.03472,
"src": "OpenStreetMap way 952346322"
},
"DE-AMP-OBERZIER": {
"id": "DE-AMP-OBERZIER",
"name": "Oberzier",
"cc": "DE",
"tso": "Amprion",
"lat": 50.87182,
"lon": 6.45779,
"src": "OpenStreetMap way 26593929"
},
"AT-APG-TAUERN": {
"id": "AT-APG-TAUERN",
"name": "Tauern",
"cc": "AT",
"tso": "APG",
"lat": 47.27772,
"lon": 12.73981,
"src": "OpenStreetMap way 38868243"
},
"AT-APG-ERNSTHOFEN": {
"id": "AT-APG-ERNSTHOFEN",
"name": "Ernsthofen",
"cc": "AT",
"tso": "APG",
"lat": 48.1246,
"lon": 14.47376,
"src": "OpenStreetMap way 27763479"
},
"AT-APG-TERNITZ": {
"id": "AT-APG-TERNITZ",
"name": "Ternitz",
"cc": "AT",
"tso": "APG",
"lat": 47.70709,
"lon": 16.06499,
"src": "OpenStreetMap way 83202471"
},
"AT-APG-YBBSFELD": {
"id": "AT-APG-YBBSFELD",
"name": "Ybbsfeld",
"cc": "AT",
"tso": "APG",
"lat": 48.1467,
"lon": 15.04782,
"src": "OpenStreetMap way 30076588"
},
"AT-APG-STPETER": {
"id": "AT-APG-STPETER",
"name": "St. Peter am Hart",
"cc": "AT",
"tso": "APG",
"lat": 48.25633,
"lon": 13.082,
"src": "OpenStreetMap way 81540198"
},
"AT-APG-LIENZ": {
"id": "AT-APG-LIENZ",
"name": "Lienz",
"cc": "AT",
"tso": "APG",
"lat": 46.82509,
"lon": 12.80634,
"src": "OpenStreetMap way 162826525"
},
"AT-APG-NAUDERS": {
"id": "AT-APG-NAUDERS",
"name": "Nauders",
"cc": "AT",
"tso": "APG",
"lat": 47.25398,
"lon": 10.88241,
"src": "JAO Static Grid Model map"
},
"CZ-CEPS-HRADEC": {
"id": "CZ-CEPS-HRADEC",
"name": "Hradec u Kadaně",
"cc": "CZ",
"tso": "ČEPS",
"lat": 50.34592,
"lon": 13.32731,
"src": "OpenStreetMap way 28099187"
},
"CZ-CEPS-NOSOVICE": {
"id": "CZ-CEPS-NOSOVICE",
"name": "Nošovice",
"cc": "CZ",
"tso": "ČEPS",
"lat": 49.66903,
"lon": 18.4309,
"src": "OpenStreetMap way 28624204"
},
"CZ-CEPS-PROSENICE": {
"id": "CZ-CEPS-PROSENICE",
"name": "Prosenice",
"cc": "CZ",
"tso": "ČEPS",
"lat": 49.50366,
"lon": 17.49922,
"src": "OpenStreetMap way 28625556"
},
"CZ-CEPS-CECHYSTRED": {
"id": "CZ-CEPS-CECHYSTRED",
"name": "Čechy Střed",
"cc": "CZ",
"tso": "ČEPS",
"lat": 50.14306,
"lon": 14.76858,
"src": "OpenStreetMap way 28468576"
},
"CZ-CEPS-LISKOVEC": {
"id": "CZ-CEPS-LISKOVEC",
"name": "Lískovec",
"cc": "CZ",
"tso": "ČEPS",
"lat": 49.70515,
"lon": 18.31596,
"src": "OpenStreetMap way 28624004"
},
"CZ-CEPS-SOKOLNICE": {
"id": "CZ-CEPS-SOKOLNICE",
"name": "Sokolnice",
"cc": "CZ",
"tso": "ČEPS",
"lat": 49.11206,
"lon": 16.69948,
"src": "OpenStreetMap way 27849030"
},
"SI-ELES-PODLOG": {
"id": "SI-ELES-PODLOG",
"name": "Podlog",
"cc": "SI",
"tso": "ELES",
"lat": 46.27603,
"lon": 15.1297,
"src": "OpenStreetMap way 119244495"
},
"SI-ELES-DIVACA": {
"id": "SI-ELES-DIVACA",
"name": "Divača",
"cc": "SI",
"tso": "ELES",
"lat": 45.69129,
"lon": 13.9689,
"src": "OpenStreetMap way 96747435"
},
"BE-ELIA-VANEYCK": {
"id": "BE-ELIA-VANEYCK",
"name": "Van Eyck (Kinrooi)",
"cc": "BE",
"tso": "Elia",
"lat": 51.12104,
"lon": 5.77422,
"src": "OpenStreetMap way 124703985"
},
"BE-ELIA-ZANDVLIET": {
"id": "BE-ELIA-ZANDVLIET",
"name": "Zandvliet",
"cc": "BE",
"tso": "Elia",
"lat": 51.36977,
"lon": 4.24827,
"src": "OpenStreetMap way 23136343"
},
"BE-ELIA-AVELGEM": {
"id": "BE-ELIA-AVELGEM",
"name": "Avelgem",
"cc": "BE",
"tso": "Elia",
"lat": 50.78257,
"lon": 3.46927,
"src": "OpenStreetMap way 87529551"
},
"BE-ELIA-COURCELLES": {
"id": "BE-ELIA-COURCELLES",
"name": "Courcelles",
"cc": "BE",
"tso": "Elia",
"lat": 50.49428,
"lon": 4.34821,
"src": "OpenStreetMap way 87772593"
},
"BE-ELIA-HORTA": {
"id": "BE-ELIA-HORTA",
"name": "Horta (Zomergem)",
"cc": "BE",
"tso": "Elia",
"lat": 51.11471,
"lon": 3.60457,
"src": "OpenStreetMap way 495688597"
},
"BE-ELIA-DOEL": {
"id": "BE-ELIA-DOEL",
"name": "Doel",
"cc": "BE",
"tso": "Elia",
"lat": 51.32275,
"lon": 4.25478,
"src": "OpenStreetMap way 23136868"
},
"BE-ELIA-BRUEGEL": {
"id": "BE-ELIA-BRUEGEL",
"name": "Bruegel (Dilbeek)",
"cc": "BE",
"tso": "Elia",
"lat": 50.88098,
"lon": 4.23169,
"src": "OpenStreetMap way 23175771"
},
"BE-ELIA-AUBANGE": {
"id": "BE-ELIA-AUBANGE",
"name": "Aubange",
"cc": "BE",
"tso": "Elia",
"lat": 49.56647,
"lon": 5.7958,
"src": "OpenStreetMap way 121085952"
},
"BE-ELIA-BRUME": {
"id": "BE-ELIA-BRUME",
"name": "Brume (Trois-Ponts)",
"cc": "BE",
"tso": "Elia",
"lat": 50.37559,
"lon": 5.84339,
"src": "OpenStreetMap way 88799384"
},
"BE-ELIA-MONCEAU": {
"id": "BE-ELIA-MONCEAU",
"name": "Monceau",
"cc": "BE",
"tso": "Elia",
"lat": 50.39365,
"lon": 4.37167,
"src": "OpenStreetMap way 103455240"
},
"BE-ELIA-GRAMME": {
"id": "BE-ELIA-GRAMME",
"name": "Gramme",
"cc": "BE",
"tso": "Elia",
"lat": 50.51229,
"lon": 5.28656,
"src": "OpenStreetMap way 87772591"
},
"BE-ELIA-MERCATOR": {
"id": "BE-ELIA-MERCATOR",
"name": "Mercator",
"cc": "BE",
"tso": "Elia",
"lat": 51.15426,
"lon": 4.24606,
"src": "OpenStreetMap way 23151789"
},
"BE-ELIA-LIXHE": {
"id": "BE-ELIA-LIXHE",
"name": "Lixhe",
"cc": "BE",
"tso": "Elia",
"lat": 50.75442,
"lon": 5.66802,
"src": "OpenStreetMap way 23141556"
},
"BE-ELIA-ACHENE": {
"id": "BE-ELIA-ACHENE",
"name": "Achêne",
"cc": "BE",
"tso": "Elia",
"lat": 50.26061,
"lon": 5.05564,
"src": "OpenStreetMap way 88373135"
},
"HU-MAVIR-GYOR": {
"id": "HU-MAVIR-GYOR",
"name": "Győr",
"cc": "HU",
"tso": "MAVIR",
"lat": 47.67316,
"lon": 17.69359,
"src": "OpenStreetMap way 124244078"
},
"HU-MAVIR-GOD": {
"id": "HU-MAVIR-GOD",
"name": "Göd",
"cc": "HU",
"tso": "MAVIR",
"lat": 47.69236,
"lon": 19.1622,
"src": "OpenStreetMap way 100872886"
},
"HU-MAVIR-GONYU": {
"id": "HU-MAVIR-GONYU",
"name": "Gönyű",
"cc": "HU",
"tso": "MAVIR",
"lat": 47.73443,
"lon": 17.80484,
"src": "OpenStreetMap way 142106943"
},
"PL-PSE-MIKULOWA": {
"id": "PL-PSE-MIKULOWA",
"name": "Mikułowa",
"cc": "PL",
"tso": "PSE",
"lat": 51.09096,
"lon": 15.11204,
"src": "OpenStreetMap way 111615226"
},
"PL-PSE-JOACHIMOW": {
"id": "PL-PSE-JOACHIMOW",
"name": "Joachimów",
"cc": "PL",
"tso": "PSE",
"lat": 50.75996,
"lon": 19.31669,
"src": "OpenStreetMap way 241842600"
},
"PL-PSE-GRUDZIADZ": {
"id": "PL-PSE-GRUDZIADZ",
"name": "Grudziądz",
"cc": "PL",
"tso": "PSE",
"lat": 53.44691,
"lon": 18.8045,
"src": "OpenStreetMap way 46092396"
},
"FR-RTE-AVELIN": {
"id": "FR-RTE-AVELIN",
"name": "Avelin",
"cc": "FR",
"tso": "RTE",
"lat": 50.52493,
"lon": 3.08229,
"src": "OpenStreetMap way 85605950"
},
"FR-RTE-VIGY": {
"id": "FR-RTE-VIGY",
"name": "Vigy",
"cc": "FR",
"tso": "RTE",
"lat": 49.19791,
"lon": 6.27672,
"src": "OpenStreetMap way 81641090"
},
"FR-RTE-LONNY": {
"id": "FR-RTE-LONNY",
"name": "Lonny",
"cc": "FR",
"tso": "RTE",
"lat": 49.82748,
"lon": 4.57287,
"src": "OpenStreetMap way 82825438"
},
"FR-RTE-MASTAING": {
"id": "FR-RTE-MASTAING",
"name": "Mastaing",
"cc": "FR",
"tso": "RTE",
"lat": 50.29636,
"lon": 3.29078,
"src": "OpenStreetMap way 82967739"
},
"FR-RTE-MERY": {
"id": "FR-RTE-MERY",
"name": "Méry-sur-Seine",
"cc": "FR",
"tso": "RTE",
"lat": 48.51977,
"lon": 3.91835,
"src": "OpenStreetMap way 82599261"
},
"FR-RTE-BEZAUMONT": {
"id": "FR-RTE-BEZAUMONT",
"name": "Bezaumont",
"cc": "FR",
"tso": "RTE",
"lat": 48.85727,
"lon": 6.09772,
"src": "OpenStreetMap way 81665271"
},
"FR-RTE-CHEVALET": {
"id": "FR-RTE-CHEVALET",
"name": "Chevalet",
"cc": "FR",
"tso": "RTE",
"lat": 50.18968,
"lon": 2.6483,
"src": "JAO Static Grid Model map"
},
"FR-RTE-GAVRELLE": {
"id": "FR-RTE-GAVRELLE",
"name": "Gavrelle",
"cc": "FR",
"tso": "RTE",
"lat": 50.33727,
"lon": 2.88522,
"src": "OpenStreetMap way 117893799"
},
"FR-RTE-MOULAINE": {
"id": "FR-RTE-MOULAINE",
"name": "Moulaine",
"cc": "FR",
"tso": "RTE",
"lat": 49.48311,
"lon": 5.79173,
"src": "OpenStreetMap way 225210595"
},
"FR-RTE-MAZURES": {
"id": "FR-RTE-MAZURES",
"name": "Les Mazures",
"cc": "FR",
"tso": "RTE",
"lat": 49.90561,
"lon": 4.62601,
"src": "OpenStreetMap way 82949228"
},
"FR-RTE-CAPELLE": {
"id": "FR-RTE-CAPELLE",
"name": "La Capelle",
"cc": "FR",
"tso": "RTE",
"lat": 49.97356,
"lon": 3.93555,
"src": "OpenStreetMap way 117975440"
},
"FR-RTE-CHOOZ": {
"id": "FR-RTE-CHOOZ",
"name": "Chooz",
"cc": "FR",
"tso": "RTE",
"lat": 50.08749,
"lon": 4.79167,
"src": "OpenStreetMap way 118655844"
},
"FR-RTE-MUHLBACH": {
"id": "FR-RTE-MUHLBACH",
"name": "Muhlbach",
"cc": "FR",
"tso": "RTE",
"lat": 47.9071,
"lon": 7.55579,
"src": "OpenStreetMap way 32196554"
},
"FR-RTE-SCHEER": {
"id": "FR-RTE-SCHEER",
"name": "Scheer",
"cc": "FR",
"tso": "RTE",
"lat": 48.3567,
"lon": 7.51904,
"src": "OpenStreetMap way 214483816"
},
"SK-SEPS-SUCANY": {
"id": "SK-SEPS-SUCANY",
"name": "Sučany",
"cc": "SK",
"tso": "SEPS",
"lat": 49.09069,
"lon": 18.97598,
"src": "OpenStreetMap way 239560640"
},
"SK-SEPS-VARIN": {
"id": "SK-SEPS-VARIN",
"name": "Varín",
"cc": "SK",
"tso": "SEPS",
"lat": 49.21364,
"lon": 18.863,
"src": "OpenStreetMap way 672771472"
},
"SK-SEPS-GABCIKOVO": {
"id": "SK-SEPS-GABCIKOVO",
"name": "Gabčíkovo",
"cc": "SK",
"tso": "SEPS",
"lat": 47.8861,
"lon": 17.54541,
"src": "OpenStreetMap way 648002755"
},
"SK-SEPS-PBISKUPICE": {
"id": "SK-SEPS-PBISKUPICE",
"name": "Podunajské Biskupice",
"cc": "SK",
"tso": "SEPS",
"lat": 48.11268,
"lon": 17.21776,
"src": "OpenStreetMap way 5131796"
},
"SK-SEPS-VOLA": {
"id": "SK-SEPS-VOLA",
"name": "Voľa",
"cc": "SK",
"tso": "SEPS",
"lat": 48.85752,
"lon": 21.8483,
"src": "OpenStreetMap way 556969469"
},
"SK-SEPS-VKAPUSANY": {
"id": "SK-SEPS-VKAPUSANY",
"name": "Veľké Kapušany",
"cc": "SK",
"tso": "SEPS",
"lat": 48.5688,
"lon": 22.04019,
"src": "OpenStreetMap way 175052362"
},
"RO-TEL-ARAD": {
"id": "RO-TEL-ARAD",
"name": "Arad",
"cc": "RO",
"tso": "Transelectrica",
"lat": 46.18324,
"lon": 21.3781,
"src": "OpenStreetMap way 101158766"
},
"RO-TEL-URECHESTI": {
"id": "RO-TEL-URECHESTI",
"name": "Urechești",
"cc": "RO",
"tso": "Transelectrica",
"lat": 44.97135,
"lon": 23.25697,
"src": "OpenStreetMap way 199657972"
},
"RO-TEL-MINTIA": {
"id": "RO-TEL-MINTIA",
"name": "Mintia",
"cc": "RO",
"tso": "Transelectrica",
"lat": 45.9166,
"lon": 22.83163,
"src": "OpenStreetMap way 97402111"
},
"DE-TTG-DIELE": {
"id": "DE-TTG-DIELE",
"name": "Diele",
"cc": "DE",
"tso": "TenneT DE",
"lat": 53.12595,
"lon": 7.31153,
"src": "OpenStreetMap way 177829920"
},
"DE-TTG-GRAFENRHEINFELD": {
"id": "DE-TTG-GRAFENRHEINFELD",
"name": "Grafenrheinfeld",
"cc": "DE",
"tso": "TenneT DE",
"lat": 49.98117,
"lon": 10.18831,
"src": "OpenStreetMap way 108797675"
},
"DE-TTG-BERGRHEINFELD": {
"id": "DE-TTG-BERGRHEINFELD",
"name": "Bergrheinfeld/West",
"cc": "DE",
"tso": "TenneT DE",
"lat": 49.98212,
"lon": 10.15086,
"src": "OpenStreetMap way 499165542"
},
"DE-TTG-DOERPENWEST": {
"id": "DE-TTG-DOERPENWEST",
"name": "Dörpen/West",
"cc": "DE",
"tso": "TenneT DE",
"lat": 52.98086,
"lon": 7.25669,
"src": "OpenStreetMap way 142487746"
},
"DE-TTG-PLEINTING": {
"id": "DE-TTG-PLEINTING",
"name": "Pleinting",
"cc": "DE",
"tso": "TenneT DE",
"lat": 48.66707,
"lon": 13.10315,
"src": "OpenStreetMap way 24976300"
},
"DE-TTG-SITTLING": {
"id": "DE-TTG-SITTLING",
"name": "Sittling",
"cc": "DE",
"tso": "TenneT DE",
"lat": 48.83809,
"lon": 11.7924,
"src": "OpenStreetMap way 24759324"
},
"DE-TTG-KREMPERMARSCH": {
"id": "DE-TTG-KREMPERMARSCH",
"name": "Krempermarsch",
"cc": "DE",
"tso": "TenneT DE",
"lat": 53.84799,
"lon": 9.50208,
"src": "OpenStreetMap way 1107961833"
},
"DE-TTG-WUERGAU": {
"id": "DE-TTG-WUERGAU",
"name": "Würgau",
"cc": "DE",
"tso": "TenneT DE",
"lat": 49.97879,
"lon": 11.10678,
"src": "OpenStreetMap way 24757393"
},
"NL-TTB-MEEDEN": {
"id": "NL-TTB-MEEDEN",
"name": "Meeden",
"cc": "NL",
"tso": "TenneT NL",
"lat": 53.12383,
"lon": 6.94995,
"src": "OpenStreetMap way 167695568"
},
"NL-TTB-DIEMEN": {
"id": "NL-TTB-DIEMEN",
"name": "Diemen",
"cc": "NL",
"tso": "TenneT NL",
"lat": 52.33676,
"lon": 5.01454,
"src": "OpenStreetMap way 934114405"
},
"NL-TTB-RILLAND": {
"id": "NL-TTB-RILLAND",
"name": "Rilland",
"cc": "NL",
"tso": "TenneT NL",
"lat": 51.42386,
"lon": 4.21871,
"src": "OpenStreetMap way 577158336"
},
"NL-TTB-MAASBRACHT": {
"id": "NL-TTB-MAASBRACHT",
"name": "Maasbracht",
"cc": "NL",
"tso": "TenneT NL",
"lat": 51.14897,
"lon": 5.91972,
"src": "OpenStreetMap way 113205679"
},
"NL-TTB-LELYSTAD": {
"id": "NL-TTB-LELYSTAD",
"name": "Lelystad",
"cc": "NL",
"tso": "TenneT NL",
"lat": 52.57665,
"lon": 5.53885,
"src": "OpenStreetMap way 87794279"
},
"NL-TTB-DODEWAARD": {
"id": "NL-TTB-DODEWAARD",
"name": "Dodewaard",
"cc": "NL",
"tso": "TenneT NL",
"lat": 51.92688,
"lon": 5.6641,
"src": "OpenStreetMap way 87792499"
},
"NL-TTB-EINDHOVEN": {
"id": "NL-TTB-EINDHOVEN",
"name": "Eindhoven",
"cc": "NL",
"tso": "TenneT NL",
"lat": 51.4464,
"lon": 5.53257,
"src": "OpenStreetMap way 162179284"
},
"NL-TTB-ZWOLLE": {
"id": "NL-TTB-ZWOLLE",
"name": "Zwolle",
"cc": "NL",
"tso": "TenneT NL",
"lat": 52.52989,
"lon": 6.19002,
"src": "OpenStreetMap way 87354306"
},
"NL-TTB-ENS": {
"id": "NL-TTB-ENS",
"name": "Ens",
"cc": "NL",
"tso": "TenneT NL",
"lat": 52.61784,
"lon": 5.80858,
"src": "OpenStreetMap way 87177559"
},
"NL-TTB-HENGELO": {
"id": "NL-TTB-HENGELO",
"name": "Hengelo",
"cc": "NL",
"tso": "TenneT NL",
"lat": 52.24885,
"lon": 6.75918,
"src": "OpenStreetMap way 83423799"
},
"NL-TTB-GEERTRUIDENBERG": {
"id": "NL-TTB-GEERTRUIDENBERG",
"name": "Geertruidenberg",
"cc": "NL",
"tso": "TenneT NL",
"lat": 51.70091,
"lon": 4.83334,
"src": "OpenStreetMap way 23096966"
},
"NL-TTB-DOETINCHEM": {
"id": "NL-TTB-DOETINCHEM",
"name": "Doetinchem",
"cc": "NL",
"tso": "TenneT NL",
"lat": 51.98173,
"lon": 6.25176,
"src": "OpenStreetMap way 723528920"
},
"NL-TTB-KRIMPEN": {
"id": "NL-TTB-KRIMPEN",
"name": "Krimpen a/d IJssel",
"cc": "NL",
"tso": "TenneT NL",
"lat": 51.9144,
"lon": 4.6306,
"src": "OpenStreetMap way 165242802"
},
"NL-TTB-BORSSELE": {
"id": "NL-TTB-BORSSELE",
"name": "Borssele",
"cc": "NL",
"tso": "TenneT NL",
"lat": 51.43338,
"lon": 3.72719,
"src": "OpenStreetMap way 739937233"
},
"NL-TTB-MAASVLAKTE": {
"id": "NL-TTB-MAASVLAKTE",
"name": "Maasvlakte",
"cc": "NL",
"tso": "TenneT NL",
"lat": 51.96163,
"lon": 4.02001,
"src": "OpenStreetMap way 1271576279"
},
"NL-TTB-BEVERWIJK": {
"id": "NL-TTB-BEVERWIJK",
"name": "Beverwijk",
"cc": "NL",
"tso": "TenneT NL",
"lat": 52.4599,
"lon": 4.67464,
"src": "OpenStreetMap way 352153064"
},
"NL-TTB-OOSTZAAN": {
"id": "NL-TTB-OOSTZAAN",
"name": "Oostzaan",
"cc": "NL",
"tso": "TenneT NL",
"lat": 52.42904,
"lon": 4.87618,
"src": "OpenStreetMap way 22984334"
},
"AT-TBW-BUERS": {
"id": "AT-TBW-BUERS",
"name": "Bürs",
"cc": "AT",
"tso": "TransnetBW",
"lat": 47.143,
"lon": 9.81049,
"src": "OpenStreetMap way 101997887"
},
"DE-TBW-KUEHMOOS": {
"id": "DE-TBW-KUEHMOOS",
"name": "Kühmoos",
"cc": "DE",
"tso": "TransnetBW",
"lat": 47.59598,
"lon": 7.95776,
"src": "OpenStreetMap way 88916034"
},
"DE-TBW-DELLMENSINGEN": {
"id": "DE-TBW-DELLMENSINGEN",
"name": "Dellmensingen",
"cc": "DE",
"tso": "TransnetBW",
"lat": 48.30419,
"lon": 9.88563,
"src": "OpenStreetMap way 41389594"
},
"DE-TBW-EICHSTETTEN": {
"id": "DE-TBW-EICHSTETTEN",
"name": "Eichstetten",
"cc": "DE",
"tso": "TransnetBW",
"lat": 48.09962,
"lon": 7.7539,
"src": "OpenStreetMap way 242756113"
},
"HR-HOPS-ERNESTINOVO": {
"id": "HR-HOPS-ERNESTINOVO",
"name": "Ernestinovo",
"cc": "HR",
"tso": "HOPS",
"lat": 45.47319,
"lon": 18.66332,
"src": "OpenStreetMap way 89823918"
},
"HR-HOPS-KONJSKO": {
"id": "HR-HOPS-KONJSKO",
"name": "Konjsko",
"cc": "HR",
"tso": "HOPS",
"lat": 43.60053,
"lon": 16.50084,
"src": "OpenStreetMap way 43337263"
},
"RO-TEL-TANTARENI": {
"id": "RO-TEL-TANTARENI",
"name": "Țânțăreni",
"cc": "RO",
"tso": "Transelectrica",
"lat": 44.61325,
"lon": 23.49035,
"src": "OpenStreetMap way 199657990"
},
"BG-ESO-KOZLODUY": {
"id": "BG-ESO-KOZLODUY",
"name": "Kozloduy",
"cc": "BG",
"tso": "ESO",
"lat": 43.74519,
"lon": 23.77828,
"src": "OSM way 157471230"
},
"RO-TEL-RESITA": {
"id": "RO-TEL-RESITA",
"name": "Reșița",
"cc": "RO",
"tso": "Transelectrica",
"lat": 45.34536,
"lon": 21.92149,
"src": "OpenStreetMap way 208263464"
},
"RS-EMS-PANCEVO": {
"id": "RS-EMS-PANCEVO",
"name": "Pančevo",
"cc": "RS",
"tso": "EMS",
"lat": 44.86859,
"lon": 20.69895,
"src": "OSM way 52456886"
},
"AT-APG-WIENSO": {
"id": "AT-APG-WIENSO",
"name": "Wien Südost",
"cc": "AT",
"tso": "APG",
"lat": 48.12383,
"lon": 16.41958,
"src": "OpenStreetMap way 27795895"
},
"AT-APG-NEUSIEDL": {
"id": "AT-APG-NEUSIEDL",
"name": "Neusiedl",
"cc": "AT",
"tso": "APG",
"lat": 47.96041,
"lon": 16.83798,
"src": "OpenStreetMap way 82201785"
}
}


def site_name(sid): return SITE_COORD[sid]['name'] if sid and sid in SITE_COORD else ''
def make_label_desc(r):
    n = r['name']; u = n.upper(); s1 = site_name(r['site']); s2 = site_name(r['site2'])
    t = r['type']
    stage = ' (curative variant)' if u.endswith('_CRA') else (' (preventive variant)' if u.endswith('_PRA') else '')
    if t == 'PST':
        m = re.match(r'^(?:PST|AT)[ _](?:.*?)(?:[ _]([A-Z]?\d{1,3}[A-Z]?|D\w|PF\d|TD\d))?(?:_(?:PRA|CRA))?$', n, re.I)
        unit = ''
        mm = re.search(r'(T?\d{3}[AB]?|D[0-9A]|PF\d|TD\d|_\d(?=_|$)|400/220_\d|BMT\d+)(?:_(?:PRA|CRA))?$', n, re.I)
        if mm: unit = mm.group(1).strip('_')
        kind = 'phase-shifting autotransformer' if u.startswith('AT') else 'phase-shifting transformer'
        unit = unit.replace('400/220_', 'T')
        label = f"PST {s1}" + (f" {unit}" if unit else '')
        if u.startswith('AT'): label = f"AT {s1} {unit}".strip()
        desc = f"Tap change on {kind}{(' ' + unit) if unit else ''} at {s1}{stage}. The tap columns show the PST tap position in the common grid model before and after the non-costly remedial action optimisation (NRAO)."
        return label, desc
    if t == 'TAP':
        mm = re.search(r'(BMT\d+|\d{3})$', n); unit = mm.group(1) if mm else ''
        return f"Transformer {s1} {unit}".strip(), f"Tap change on transformer {unit} at {s1}."
    if t == 'LINE':
        verb = 'switched in' if 'CLOSE' in u else 'switched out'
        mm = re.search(r'_c(\d)$', n) or re.search(r'_(\d{2})_(?:CRA|PRA)$', n)
        circ = f" (circuit {mm.group(1)})" if mm else ''
        if 'PLEINTING' in u:
            return 'St. Peter – Pleinting Tr3', 'Topological action on the St. Peter (AT) – Pleinting (DE) interconnection involving transformer 3 at St. Peter.'
        return f"{s1} – {s2} line {verb.split()[1]}", f"The {s1} – {s2} line{circ} is {verb}{stage}."
    if t == 'MERGE':
        return f"{s1} 1-node", f"Bus-bars at {s1} coupled into a single node{stage}."
    # splits / couplers / complex
    detail = ''
    m = re.search(r'quarterbar[_ ]?([0-9][.]?[0-9]?[AB]?)', n, re.I)
    if m: detail = f"quarter-bar {m.group(1)}"
    elif re.search(r'tronconnement', n, re.I): detail = 'bus-bar sectionalising'
    elif re.search(r'couplage(\d)?', n, re.I):
        g = re.search(r'couplage(\d)?', n, re.I).group(1); detail = 'coupler opening' + (f' {g}' if g else '')
    elif re.search(r'__(\d{2})_[A-Z]{2,4}__(\d{2})_(\d)', n):
        g = re.search(r'__(\d{2})_[A-Z]{2,4}__(\d{2})_(\d)', n); detail = f"coupler {g.group(1)}–{g.group(2)} opened (step {g.group(3)})"
    elif re.search(r'_AT(\d{3})', n): detail = 'autotransformer coupler ' + re.search(r'_AT(\d{3})', n).group(1)
    elif re.search(r'TOP_2NH', u): detail = 'horizontal split'
    elif re.search(r'TOP_2NV', u): detail = 'vertical split'
    elif re.search(r'TOP_2N_(\d)_', u): detail = 'coupler ' + re.search(r'TOP_2N_(\d)_', u).group(1)
    if u.startswith('TOP_COMPLEX') and r['tso'] == 'MAVIR':
        toks = re.sub(r'^TOP_COMPLEX_', '', n, flags=re.I); toks = re.sub(r'_(PRA|CRA)$', '', toks).split('_')[1:]
        names = {'SZHO': 'Szombathely', 'ZURN': 'Zurndorf', 'GABC': 'Gabčíkovo', 'LITR': 'Litér', 'AISA': 'Albertirsa', 'LEVIC': 'Levice',
                 'SAJO': 'Sajóivánka', 'GYOR': 'Győr', 'BICSD': 'Bicske Dél', 'VDUR': 'Veľký Ďur', 'WIEN': 'Wien Südost', 'NEUS': 'Neusiedl'}
        feeders = ', '.join(names.get(x.upper(), x) for x in toks)
        short = '/'.join(names.get(x.upper(), x).split('–')[0].split(' ')[0] for x in toks)
        return f"{s1} split · {short}", f"Multi-node bus-bar configuration at {s1} separating the {feeders} feeders{stage}."
    if re.match(r'^TOP_(ERNESTINOVO|KONJSKO)', u):
        return f"{s1} topology", f"Topological switching action at the 400 kV {s1} substation (configuration {n[-1]})."
    if 'MINTIA' in u:
        return f"{s1} TR3 in", f"Second 400/220 kV transformer at {s1} switched in (normally one transformer in operation)."
    label = f"{s1} split" + (f" · {detail}" if detail else '')
    desc = f"Bus-bar topology change at {s1}" + (f" ({detail})" if detail else '') + f": the substation is operated as two electrical nodes instead of one{stage}."
    if s2 and r['type'] == 'SPLIT' and s2 != s1:
        desc += f" Also involves {s2}."
    return label, desc


# ---------- geolocation of CNECs and new substations (JAO Static Grid Model + OpenStreetMap) ----------
_HERE = os.path.dirname(os.path.abspath(__file__))
try:
    _GEO = json.load(open(os.path.join(_HERE, 'geo.json'), encoding='utf-8'))
except FileNotFoundError:
    _GEO = dict(coord={}, lines={}, extra={}, tso_map={})
COORD = {k: tuple(v) for k, v in _GEO['coord'].items()}
LINES = _GEO['lines']
EXTRA_POINTS = {k: (tuple(v) if v else None) for k, v in _GEO['extra'].items()}
TSO_MAP = _GEO['tso_map']
def _ascii(s): return re.sub(r'[^a-z0-9]', '', unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode().lower())
NAME_IDX = collections.defaultdict(list)
for _k in COORD: NAME_IDX[_ascii(_k.split('|', 1)[1])].append(_k)
TSO_CC = {'50HERTZ': 'DE', 'AMPRION GMBH': 'DE', 'APG': 'AT', 'CEPS': 'CZ', 'ELES': 'SI', 'ELIA': 'BE', 'HOPS': 'HR', 'MAVIR': 'HU',
          'PSE': 'PL', 'RTE': 'FR', 'SEPS': 'SK', 'TEL': 'RO', 'TENNETGMBH': 'DE', 'TENNET NL': 'NL', 'TRANSNETBW': 'DE', 'CREOS': 'LU'}
def km(a, b):
    import math
    R = 6371; p1, p2 = math.radians(a[0]), math.radians(b[0]); dp = p2 - p1; dl = math.radians(b[1] - a[1])
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))
def find_sub(name, tso):
    n = _ascii(name)
    if n in EXTRA_POINTS and EXTRA_POINTS[n]: return EXTRA_POINTS[n]
    if not n: return None
    keys = NAME_IDX.get(n, [])
    if not keys:
        keys = [k for kk, ks in NAME_IDX.items() if kk.startswith(n) and len(n) >= 4 for k in ks]
    if not keys: return None
    keys = sorted(keys, key=lambda k: (not k.startswith(tso + '|'), len(k)))
    return COORD.get(keys[0])
SUFFIX_RE = re.compile(r'\s+([A-Z]{6,10}|\d{2,3}([.\-/]\S+)?|[A-Z]{3,8}\d?\s+[A-Z]{1,2}\d?|\d|c\.?\s?\d|rt|ws|sw|gr|bl|W|O|N|S|Z|NO|NW|SO|SW|WS|ZW|ZO|WB|[IVX]+\.?|TR\s?\d+(\s+\w)?|(Black|White|Red|Blue|Wit|Zwart|Rood|Blauw)(/\d+)?|\d{3}\s+[A-Z])(\s.*)?$')
def clean_end(B):
    B = re.sub(r'\s*\([^)]*\)\s*$', '', B.strip())
    for _ in range(3):
        B2 = SUFFIX_RE.sub('', B)
        if B2 == B: break
        B = B2
    return B.strip(' -–()')
def _point(p): return [round(p[0], 4), round(p[1], 4), round(p[0], 4), round(p[1], 4)]
def geo_element(text, tso_raw):
    """[lat1, lon1, lat2, lon2] for a grid element name (equal pairs = a point such as a transformer), or None."""
    tso = TSO_MAP.get(norm(tso_raw), '')
    t = text.strip()
    k = re.sub(r'[^a-z0-9]', '', t.lower())
    if k in LINES:
        l = sorted(LINES[k], key=lambda l: l[0] != tso)[0]
        if l[1] and l[2]: return [round(l[1][0], 4), round(l[1][1], 4), round(l[2][0], 4), round(l[2][1], 4)]
    s = re.sub(r'^(L|LIT) \d+kV N0 \d+ ', '', t)
    s = re.sub(r'^\d+\s?kV\s+', '', s)
    m = re.match(r'^(?:PST|TR|AT|Trafo)\s+(.+?)(?:\s+\d+)?$', s) or re.match(r'^(.+?)\s+(?:AT|TR|PST|T)\s?\d+[A-Z]?$', s)
    if m and ' - ' not in s:
        p = find_sub(clean_end(m.group(1)), tso)
        if p: return _point(p)
    if re.match(r'^Y\s*[-–]', s) and '(' in s:
        s = s[s.index('(') + 1:].replace(')', ' ').strip().lstrip('-– ')
    s = re.sub(r'\s*\([^)]*\)\s*$', '', s)
    parts = [p for p in re.split(r'\s+[-–]\s+|(?<=[a-zA-Z])-(?=[A-Z])|\s+_\s+', s) if p.strip()]
    if len(parts) >= 2:
        A = clean_end(re.sub(r'^(Y|T)[\s-]+', '', parts[0].strip()))
        B = clean_end(parts[-1].strip())
        pa, pb = find_sub(A, tso), find_sub(B, tso)
        if pa and pb and km(pa, pb) < 400:
            return [round(pa[0], 4), round(pa[1], 4), round(pb[0], 4), round(pb[1], 4)]
    elif len(parts) == 1:
        p = find_sub(clean_end(parts[0]), tso)
        if p: return _point(p)
    return None
def auto_site(ra_name, tso_raw):
    """Place a remedial action at a substation of the Static Grid Model whose name appears in the RA name."""
    body = re.sub(r'^(PST|AT|TR|TOP|SPS)[ _]+', '', ra_name, flags=re.I)
    toks = [t for t in re.split(r'[_\s/\-.]+', body) if len(t) >= 4 and not re.match(r'^(PRA|CRA|OPEN|CLOSE|COMPLEX|NODE|NODES|COUPLAGE|TRONCONNEMENT|QUARTERBAR.*|T?\d+[A-Z]?|[A-Z]?\d+)$', t, re.I)]
    tso = TSO_MAP.get(norm(tso_raw or ''), '')
    for t in toks:
        keys = sorted(NAME_IDX.get(_ascii(t), []), key=lambda k: (not k.startswith(tso + '|'), len(k)))
        if not keys: continue
        k = keys[0]; ktso, nm = k.split('|', 1)
        sid = 'AUTO-' + re.sub(r'[^A-Z0-9]+', '-', k.upper()).strip('-')
        lat, lon = COORD[k]
        SITE_COORD[sid] = dict(id=sid, name=nm.title() if nm.isupper() else nm, cc=TSO_CC.get(ktso, ''),
                               tso=tso_display(tso_raw) if tso_raw else ktso, lat=round(lat, 5), lon=round(lon, 5),
                               src='JAO Static Grid Model map (automatic name match)')
        return sid
    return None

TZ = zoneinfo.ZoneInfo('Europe/Brussels')
LINE_TYPES = {'OPEN', 'CLOSE'}

def new_ra(i, name, tsos):
    typ, kind = classify(name)
    sids = [s for s in sites_in(name) if s in SITE_COORD]
    if not sids:
        a = auto_site(name, sorted(tsos)[0] if tsos else '')
        if a: sids = [a]
    tso = tso_display(sorted(tsos)[0]) if tsos else (SITE_COORD[sids[0]]['tso'] if sids else '')
    site, site2 = (sids[0] if sids else None), None
    if len(sids) >= 2 and typ in LINE_TYPES:
        site, site2 = sids[0], sids[1]
        kind = 'Line switched out' if typ == 'OPEN' else 'Line switched in'
        typ = 'LINE'
    elif typ == 'OPEN':
        kind, typ = 'Bus-bar coupler opened', 'SPLIT'
    r = dict(i=i, name=name, tso=tso, type=typ, kind=kind, site=site, site2=site2, desc='', sgm='', nP=0, nC=0, nCh=0,
             first='', last='', tmin=None, tmax=None)
    lab, d = make_label_desc(r)
    r['label'], r['auto'] = lab, d
    if not site:
        r['label'] = name
        r['auto'] = 'This remedial action is not on the map yet: its substation could not be identified from the name.'
    return r

def read_tool_result(path):
    d = json.load(open(path))
    t = d[0]['text'] if isinstance(d, list) else d
    k = t.rfind('\n\n(captured at origin')
    if k > 0: t = t[:k]
    t = t.strip()
    return json.loads(t) if t.startswith('"') else t

def load_chunks(paths):
    parts, n, sha = {}, None, None
    for p in paths:
        s = read_tool_result(p)
        tag, i, n, sha, pad, data = s.split('|', 5)
        assert tag == 'JAODUMP', p
        parts[int(i)] = data
    n = int(n)
    missing = [i for i in range(n) if i not in parts]
    if missing: sys.exit(f'missing chunks: {missing}')
    raw = base64.b64decode(''.join(parts[i] for i in range(n)))
    if hashlib.sha256(raw).hexdigest() != sha: sys.exit('checksum mismatch: the chunks do not belong together')
    return json.loads(gzip.decompress(raw))

def local_midnight_utc(day):
    d = datetime.date.fromisoformat(day)
    return datetime.datetime(d.year, d.month, d.day, tzinfo=TZ).astimezone(datetime.timezone.utc)

def fetch(from_day, to_day, log):
    start = local_midnight_utc(from_day)
    end = local_midnight_utc((datetime.date.fromisoformat(to_day) + datetime.timedelta(days=1)).isoformat())
    D = dict(ra=[], raTso={}, cnec=[], reasons=[], pra=[], cra=[])
    idx = {'ra': {}, 'cnec': {}, 'reasons': {}}
    def ix(kind, v):
        m = idx[kind]
        if v not in m: m[v] = len(D[kind]); D[kind].append(v)
        return m[v]
    P, C = {}, {}
    def get(ep, f, t):
        url = f'https://publicationtool.jao.eu/core/api/data/{ep}?FromUtc={f:%Y-%m-%dT%H:%M:%S.000Z}&ToUtc={t:%Y-%m-%dT%H:%M:%S.000Z}'
        for k in range(10):
            try:
                with urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'core-ra-map-updater'}), timeout=60) as r:
                    return json.load(r)
            except urllib.error.HTTPError as e:
                if e.code == 429: time.sleep(int(e.headers.get('Retry-After', '60')) + 2); continue
                if e.code == 400: return {'data': []}
                time.sleep(3 * (k + 1))
            except Exception:
                time.sleep(3 * (k + 1))
        raise RuntimeError('JAO API unreachable: ' + url)
    t = start
    while t < end:
        u = min(t + datetime.timedelta(hours=48), end)
        p = get('pra', t, u); time.sleep(0.4); c = get('cra', t, u); time.sleep(0.4)
        for x in p.get('data') or []:
            h = round(datetime.datetime.fromisoformat(x['dateTimeUtc'].replace('Z', '+00:00')).timestamp() / 3600)
            e = P.setdefault(h, [-1, []])
            if x.get('reason'): e[0] = ix('reasons', x['reason'])
            if x.get('name'):
                i = ix('ra', x['name'])
                if x.get('tso'): D['raTso'].setdefault(str(i), {})[x['tso']] = 1
                e[1].append([i, x.get('baseline'), x.get('afterNrao')])
        for x in c.get('data') or []:
            h = round(datetime.datetime.fromisoformat(x['dateTimeUtc'].replace('Z', '+00:00')).timestamp() / 3600)
            e = C.setdefault(h, [-1, []])
            if x.get('reason'): e[0] = ix('reasons', x['reason'])
            if x.get('cnecName'):
                row = [ix('cnec', (x.get('cnecTso') or '') + '|' + x['cnecName'])]
                for k in range(1, 11):
                    n = x.get(f'cra{k}Name')
                    if n: row += [ix('ra', n), x.get(f'cra{k}Baseline'), x.get(f'cra{k}AfterNrao')]
                e[1].append(row)
        log(f'fetched {t:%Y-%m-%d %H:%M} -> {u:%Y-%m-%d %H:%M} UTC')
        t = u
    D['pra'] = [[h, e[0], e[1]] for h, e in sorted(P.items())]
    D['cra'] = [[h, e[0], e[1]] for h, e in sorted(C.items())]
    return D

def hiso(h): return datetime.datetime.fromtimestamp(h * 3600, datetime.timezone.utc).strftime('%Y-%m-%d')
def mkey(h): return datetime.datetime.fromtimestamp(h * 3600, datetime.timezone.utc).strftime('%Y-%m')

def merge(site, D):
    cat = json.load(open(os.path.join(site, 'catalog.json')))
    ras, cnecs, reasons = cat['ras'], cat['cnecs'], cat['reasons']
    ra_ix = {r['name']: r['i'] for r in ras}
    cn_ix = {}
    for k, c in enumerate(cnecs):
        cn_ix.setdefault(f"{c['tso']}|{c['cne']}|{c['co']}", k)
    cne_geo = {c['cne']: c['g'] for c in cnecs if c.get('g')}
    co_geo = {c['co']: c['gco'] for c in cnecs if c.get('gco')}
    rs_ix = {r: k for k, r in enumerate(reasons)}
    new_ras, new_cnecs = [], []
    def map_ra(i):
        name = D['ra'][i]
        if name not in ra_ix:
            r = new_ra(len(ras), name, list((D['raTso'].get(str(i)) or {}).keys()))
            r['m'] = [0] * len(cat['months'])
            ras.append(r); ra_ix[name] = r['i']; new_ras.append(r)
        return ra_ix[name]
    def map_cn(i):
        full = D['cnec'][i]
        tso_raw, rest = full.split('|', 1)
        cne, co = (rest.split('|', 1) + [''])[:2]
        cne, co = cne.strip(), co.strip()
        key = f"{tso_display(tso_raw)}|{cne}|{co}"
        if key not in cn_ix:
            cn_ix[key] = len(cnecs)
            co_clean = re.sub(r'^(N-1|N-2|N-k|CO)\s*', '', co)
            c = dict(tso=tso_display(tso_raw), cne=cne, co=co, g=cne_geo.get(cne) or geo_element(cne, tso_raw),
                     gco=co_geo.get(co) or (geo_element(co_clean, tso_raw) if co_clean else None))
            cnecs.append(c); new_cnecs.append(c)
        return cn_ix[key]
    def map_rs(i):
        if i is None or i < 0: return -1
        r = D['reasons'][i]
        if r not in rs_ix: rs_ix[r] = len(reasons); reasons.append(r)
        return rs_ix[r]
    hours = {}
    for h, r, rows in D['pra']:
        e = hours.setdefault(h, [h, -1, [], -1, []])
        e[1] = map_rs(r); e[2] = [[map_ra(a), b, c] for a, b, c in rows]
    for h, r, rows in D['cra']:
        e = hours.setdefault(h, [h, -1, [], -1, []])
        e[3] = map_rs(r)
        e[4] = [[map_cn(row[0])] + [v for k in range(1, len(row), 3) for v in (map_ra(row[k]), row[k + 1], row[k + 2])] for row in rows]
    # only hours that the JAO API actually returned a preventive record for are authoritative
    hours = {h: e for h, e in hours.items() if any(x[0] == h for x in D['pra'])}
    touched = collections.defaultdict(dict)
    for h, e in hours.items(): touched[mkey(h)][h] = e
    month_files = {}
    datadir = os.path.join(site, 'data')
    for f in sorted(os.listdir(datadir)):
        if f.endswith('.json'): month_files[f[:-5]] = json.load(open(os.path.join(datadir, f)))
    added = replaced = 0
    changed = []
    for mk, hs in touched.items():
        cur = {row[0]: row for row in month_files.get(mk, {'h': []})['h']}
        before = json.dumps(sorted(cur.values()), separators=(',', ':'))
        for h, e in hs.items():
            if h in cur: replaced += 1
            else: added += 1
            cur[h] = e
        after = sorted(cur.values())
        if json.dumps(after, separators=(',', ':')) != before or mk not in month_files:
            month_files[mk] = dict(m=mk, h=after); changed.append(f'data/{mk}.json')
    # ---- recompute statistics over the full history ----
    mkeys = sorted(month_files)
    nP = collections.Counter(); nC = collections.Counter(); craH = collections.defaultdict(set)
    firstH, lastH, taps = {}, {}, {}
    ra_month = collections.defaultdict(collections.Counter)
    day = {}
    for mk in mkeys:
        for h, pr, p, cr, c in month_files[mk]['h']:
            used = set()
            for ra, b, a in p:
                used.add(ra)
                for v in (b, a):
                    if v is not None:
                        lo, hi = taps.get(ra, (v, v)); taps[ra] = (min(lo, v), max(hi, v))
            for ra in set(x[0] for x in p): nP[ra] += 1
            for row in c:
                for k in range(1, len(row), 3):
                    ra = row[k]; nC[ra] += 1; craH[ra].add(h); used.add(ra)
                    for v in (row[k + 1], row[k + 2]):
                        if v is not None:
                            lo, hi = taps.get(ra, (v, v)); taps[ra] = (min(lo, v), max(hi, v))
            for ra in used:
                firstH[ra] = min(firstH.get(ra, h), h); lastH[ra] = max(lastH.get(ra, h), h); ra_month[ra][mk] += 1
            d = datetime.datetime.fromtimestamp(h * 3600, TZ).strftime('%Y-%m-%d')
            s = day.setdefault(d, [0, 0, 0, 0])
            s[0] += 1; s[1] += len(p)
            ncra = sum((len(r) - 1) // 3 for r in c)
            s[2] += ncra; s[3] += 1 if ncra else 0
    for r in ras:
        i = r['i']
        r['nP'], r['nC'], r['nCh'] = nP[i], nC[i], len(craH.get(i, ()))
        r['first'] = hiso(firstH[i]) if i in firstH else ''
        r['last'] = hiso(lastH[i]) if i in lastH else ''
        r['tmin'], r['tmax'] = taps[i] if i in taps else (None, None)
        r['m'] = [ra_month[i].get(mk, 0) for mk in mkeys]
    sizes = {}
    for mk in mkeys:
        js = json.dumps(month_files[mk], separators=(',', ':'))
        sizes[mk] = len(js)
        if f'data/{mk}.json' in changed:
            open(os.path.join(datadir, f'{mk}.json'), 'w').write(js)
    cat['months'] = [[mk, len(month_files[mk]['h']), sizes[mk]] for mk in mkeys]
    cat['days'] = [[d] + v for d, v in sorted(day.items())]
    cat['generated'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    cat['exportedAt'] = D.get('exportedAt') or cat['generated']
    used_sites = {r['site'] for r in ras + cat['extra'] if r.get('site')} | {r['site2'] for r in ras + cat['extra'] if r.get('site2')}
    have = {s['id'] for s in cat['sites']}
    for sid in sorted(used_sites - have):
        if sid in SITE_COORD: cat['sites'].append(SITE_COORD[sid])
    json.dump(cat, open(os.path.join(site, 'catalog.json'), 'w'), separators=(',', ':'), ensure_ascii=False)
    changed.append('catalog.json')
    return dict(hours_added=added, hours_replaced=replaced, last_day=cat['days'][-1][0] if cat['days'] else None,
                new_ras=[dict(name=r['name'], label=r['label'], tso=r['tso'], site=r['site']) for r in new_ras],
                unplaced_new_ras=[r['name'] for r in new_ras if not r['site']],
                new_cnecs=len(new_cnecs), new_cnecs_without_geometry=sum(1 for c in new_cnecs if not c['g']),
                changed_files=changed)

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--site', required=True, help='folder with catalog.json and data/')
    ap.add_argument('--raw-chunks', nargs='+', help='tool-result files holding JAODUMP chunks')
    ap.add_argument('--raw', help='decoded dump (.json or .json.gz)')
    ap.add_argument('--fetch', action='store_true', help='download from the JAO API directly')
    ap.add_argument('--from', dest='from_day', help='first business day to (re)load, YYYY-MM-DD (default: last day in the catalogue)')
    ap.add_argument('--to', dest='to_day', help='last business day, YYYY-MM-DD (default: tomorrow, Brussels time)')
    ap.add_argument('--range', action='store_true', help='only print the business-day range to fetch and exit')
    a = ap.parse_args()
    cat = json.load(open(os.path.join(a.site, 'catalog.json')))
    last = cat['days'][-1][0]
    from_day = a.from_day or last
    to_day = a.to_day or (datetime.datetime.now(TZ).date() + datetime.timedelta(days=1)).isoformat()
    if a.range:
        print(json.dumps(dict(from_day=from_day, to_day=to_day))); return
    if a.raw_chunks: D = load_chunks(a.raw_chunks)
    elif a.raw: D = json.load(gzip.open(a.raw) if a.raw.endswith('.gz') else open(a.raw))
    elif a.fetch: D = fetch(from_day, to_day, lambda m: print(m, file=sys.stderr))
    else: ap.error('give --raw-chunks, --raw or --fetch')
    print(json.dumps(merge(a.site, D), ensure_ascii=False, indent=1))

if __name__ == '__main__':
    main()
