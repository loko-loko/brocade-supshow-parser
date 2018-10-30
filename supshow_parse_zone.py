#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import re
import csv
import time
import pprint
import random
import shutil
import logging
import argparse
import signal

import multiprocessing as mp
from logging.handlers import RotatingFileHandler
from operator import itemgetter

### Fonctions Globales ###

def get_line_sup(lst, line, chk, rgx_in, rgx_out):
    """ Permet de récupérer un paragraphe dans le supportshow en fonction d'un regex d'entrée et de sortie """
    
    if re.search(rgx_in, line) or chk is 1:
        chk = 1
        
        if not re.search(rgx_out, line):
            lst.append(line)
            
        else:
            chk = 0
    
    return chk
    
def get_line_cmd(cmd_lst, rgx, rgx_end):
    """ Permet de récupérer un paragraphe dans une commande en fonction d'un regex d'entrée et de sortie """
    
    result_lst = []
    check = 0
    
    for l in cmd_lst:
        check = get_line_sup(result_lst, l, check, rgx, rgx_end)
    
    return result_lst
    
    
def header_maker(file_name, header_lst, delim=';'):
    """ Ecrit le header des output CSV """
    
    with open(file_name, 'w') as outfile:
        csv_writer = csv.writer(outfile, delimiter=delim)
        csv_writer.writerow(header_lst)
    
def get_master_sw(pri_file_csv):
    with open(pri_file_csv, "r") as csvfile:
        
        master_sw_dic = {}
        
        csvlines = csv.reader(csvfile, delimiter=';')
        master_sw_tup_lst = list(set([(r[0], r[1]) for r in csvlines if r[1] != '128' and r[4] == 'Yes']))
        
        master_sw_lst = list(set([m[0] for m in master_sw_tup_lst]))
        
        for master_sw in master_sw_lst:
            master_sw_dic[master_sw] = [m[1] for m in master_sw_tup_lst if master_sw == m[0]]
        
    return master_sw_dic
    
### Fonctions de Parsing ###

def get_location_info(sw_dic):
    """ Récupération des infos sur le Meta SAN et la Localisation """
    
    ### Récupération du MetaSan ###
    
    sw_dic['sw_ms'] = sw_dic['name'][1].lower()
    
    if sw_dic['sw_ms'] == 'w':
        sw_dic['sw_ms'] = '1'
        
    if re.search(r'^[a-z]b[0-9]+', sw_dic['name'].lower()):
        sw_dic['sw_ms'] = 'P'
        
    if re.search(r'^sw51', sw_dic['name'].lower()):
        sw_dic['sw_ms'] = 'S'
    
    ### Récupération du Site ###
    
    if sw_dic['sw_ms'] == 'P':
        sw_dic['sw_loc'] = sw_dic['name'][4:6].upper()
        
    else:
        sw_dic['sw_loc'] = sw_dic['name'][5:7].upper()
        
        if len(sw_dic['name']) != 10 and re.search('(N|S)0', sw_dic['sw_loc']):
            sw_dic['sw_loc'] = sw_dic['name'][4:6].upper()
    
        if re.search(r'^V(A|[0-9])', sw_dic['sw_loc']):
            sw_dic['sw_loc'] = 'VAU'
            
        elif re.search(r'^Y([0-9])', sw_dic['sw_loc']):
            sw_dic['sw_loc'] = 'VAL'
            
        elif sw_dic['sw_loc'] == 'ME':
            sw_dic['sw_loc'] = 'M7'
            
    sw_dic['sw_loc'] = 'CTI' + sw_dic['sw_loc']
    
    
def get_zone_alias_info(sw_dic, fid_dic, cfgshow_fid_lst, null_val):
    
    zone_actv_dic = {}
    alias_dic = {}
    zone_dic = {}
    active_zone = null_val
    
    check = 0
    
    for c, line in enumerate(cfgshow_fid_lst):
        
        if line != '':
            
            if check is 1:
                
                if re.search(' cfg:\t', line):
                    active_zone = line.split('\t')[1]
                    
                elif re.search(' zone:\t', line):
                    zone_actv = line.split('\t')[1]
                    zone_actv_dic[zone_actv] = []
                    
                    cnt = c + 1
                    
                    next_zone = cfgshow_fid_lst[cnt]
                    
                    while not 'zone:' in next_zone and next_zone != '':
                        zone_actv_dic[zone_actv].append(cfgshow_fid_lst[cnt].split('\t')[-1])
                        cnt += 1
                        try:
                            next_zone = cfgshow_fid_lst[cnt]
                        except IndexError:
                            break
            
            elif re.search(' zone:\t', line):
                zone_dic[line.split('\t')[1]] = cfgshow_fid_lst[c+1].split('\t')[-1].split('; ')
                
            elif re.search(' alias:\t', line):   
                alias_dic[line.split('\t')[1]] = cfgshow_fid_lst[c+1].split('\t')[-1]
                
            elif line == 'Effective configuration:':
                check = 1
                
  
    return active_zone, alias_dic, zone_dic, zone_actv_dic
    
    
def supshow_parser_exec(logger, input_path, lock, supshow_file, csv_file_dic_lst, master_sw_dic, wwn_csv_dic, null_val):
    """ Fonction de Parsing """
    
    sw_date = time.strftime("%d%m%Y")
    sw_start_time = time.time()
    
    sw_dic = {}
    sw_dic['name'] = supshow_file.split('.')[1]
    
    pid = os.getpid()
    name = mp.current_process().name
    supshow = open(input_path + '/' + supshow_file, 'r')
    supshow_lst = supshow.read().split('\n')
    
    logger.info('PID:{0}, P.NAME:{1}, SW:{2} [start]'.format(pid, name, sw_dic['name']))
    
    ### Déclaration des Listes d'item CSV ###
    
    for csv_file_dic in csv_file_dic_lst:
        exec(csv_file_dic['type'].lower() + '_csv_lst = []')
    
    ### Déclaration des Listes (_lst) et Variables (_check) de commandes (Exemple : sfpshow_lst, sfpshow_check) ###
    
    for cmd in ['fid', 'cfgshow', 'switchshow']:
        exec(cmd + '_lst = []')
        exec(cmd + '_check = 0')
    
    for line in supshow_lst:
        
        if re.search(r'CURRENT CONTEXT -- [0-9]+', line):
            fid_lst.append(line.split()[-1])
        
        cfgshow_check = get_line_sup(cfgshow_lst, line, cfgshow_check, r'^cfgshow\s+:', r'^defzone --show\s+:|zone --validate\s+:')
        switchshow_check = get_line_sup(switchshow_lst, line, switchshow_check, r'^switchshow\s+:', r'^tempshow\s+:')
        
    del supshow_lst
    supshow.close()
        
    ### Récupération des informations générales du Switch ###
    
    sw_dic['fid_lst'] = list(set(fid_lst))
    
    get_location_info(sw_dic)
    
    if not sw_dic['fid_lst']:
        sw_dic['fid_lst'].append(null_val)
    
    ### Récupération infos par FID ###
    
    for fid in sw_dic['fid_lst']:
        
        fid_dic = {}
        
        fid_dic['fid'] = fid
        
        if fid_dic['fid'] in master_sw_dic[sw_dic['name']]:
            
            ### Découpage des informations par FID ###
            
            if fid_dic['fid'] != null_val:
                
                regex_in = r'^CURRENT CONTEXT -- \d+\s+,\s+{0}$'.format(fid_dic['fid'])
                
                cfgshow_fid_lst = get_line_cmd(cfgshow_lst, regex_in, r'^cfgshow\s+:')
                switchshow_fid_lst = get_line_cmd(switchshow_lst, regex_in, r'^switchshow\s+:')
                
            else:
                cfgshow_fid_lst = cfgshow_lst
                switchshow_fid_lst = switchshow_lst
            
            ### Récupération du WWN et Nom du Switch (Virtuel) ###
            
            fid_dic['sw_name'] = [s.split('\t')[-1] for s in switchshow_fid_lst if 'switchName:' in s][0]
            fid_dic['sw_wwn'] = [s.split('\t')[-1] for s in switchshow_fid_lst if 'switchWwn:' in s][0]
            
            ### Récupération Info Zone et Alias ###
            
            active_zone, alias_dic, zone_dic, zone_actv_dic = get_zone_alias_info(sw_dic, fid_dic, cfgshow_fid_lst, null_val)
            
            if zone_actv_dic:
                
                for zone, member_lst in zone_actv_dic.items():
                    
                    for member in member_lst:
                        
                        anomaly_lst = []
                        sw_name, sw_index = (null_val, null_val)
                        
                        alias_lst = [a for a, w in alias_dic.items() if w == member]
                        
                        sw_name, sw_index = wwn_csv_dic.get(member, (null_val, null_val))
                        
                        if not alias_lst:
                            alias_lst = [null_val]
                            anomaly_lst.append('no_alias')
                            
                        if sw_name == null_val:
                            anomaly_lst.append('not_find')
                            
                        if len(alias_lst) > 1:
                            anomaly_lst.append('several_alias')
                        
                        if anomaly_lst:
                            anomaly_csv_lst.append([
                                sw_dic['name'],
                                fid_dic['fid'],
                                fid_dic['sw_name'],
                                active_zone,
                                zone,
                                member,
                                ' '.join(alias_lst),
                                ' '.join(anomaly_lst),
                                sw_name,
                                sw_index,
                                sw_date,
                            ])
                            
                        zone_csv_lst.append([
                            active_zone,
                            zone,
                            member,
                            ' '.join(alias_lst),
                            sw_name,
                            sw_index,
                            sw_date,                    
                        ]) 
                 
            ### Génération d'un fichier avec tous les Alias ###
            
            if alias_dic:
                for alias, wwn in alias_dic.items():
                    alias_csv_lst.append([sw_dic['name'], fid_dic['fid'], fid_dic['sw_name'], alias, wwn])
                
    ### Ecriture des données dans les CSV si non locké par un autre processus ###
    
    logger.info('SW:{0} Write Files'.format(sw_dic['name']))  
    
    with lock:
        for csv_file_dic in csv_file_dic_lst:
            with open(csv_file_dic['file'], 'a', 0) as outfile:
                csv_writer = csv.writer(outfile, delimiter=';')
                map(csv_writer.writerow, eval(csv_file_dic['type'].lower() + '_csv_lst'))
        
    ## Fin de la collecte ##
    
    sw_execution_time = time.strftime("%H:%M:%S", time.gmtime((time.time() - sw_start_time)))
    logger.info('PID:{0}, P.NAME:{1}, SW:{2} [done][T:{3}]'.format(pid, name, sw_dic['name'], sw_execution_time))
    
def supshow_parser_manage(logger, input_path, lock, supshow_input_lst, csv_file_dic_lst, master_sw_dic, wwn_csv_dic, null_val, debug_mode=False):
    """ Gestion de la file de switchs exécuter en sequencielle """
    
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    
    pid = os.getpid()
    name = mp.current_process().name
    queue_start_time = time.time()
    
    logger.info('PID:{0}, P.NAME:{1}, SW COUNT:{2}, SW LIST:{3} [start]'.format(pid, name, len(supshow_input_lst), ','.join([s.split('.')[1] for s in supshow_input_lst])))
    
    time.sleep(.5)
    
    for supshow_input in supshow_input_lst:
        
        if debug_mode:
            supshow_parser_exec(
                logger,
                input_path,
                lock,
                supshow_input,
                csv_file_dic_lst,
                master_sw_dic,
                wwn_csv_dic,
                null_val,
            )
            
        else:
            try:
                supshow_parser_exec(
                    logger,
                    input_path,
                    lock,
                    supshow_input,
                    csv_file_dic_lst,
                    master_sw_dic,
                    wwn_csv_dic,
                    null_val,
                )
            
            except:
                logger.error('Parsing Error on Suppshow File : {0} !'.format(supshow_input))   
        
    queue_execution_time = time.strftime("%H:%M:%S", time.gmtime((time.time() - queue_start_time)))
    
    logger.info('PID:{0}, P.NAME:{1}, SW COUNT:{2} [terminate][T:{3}]'.format(pid, name, len(supshow_input_lst), queue_execution_time))
    
if __name__ == '__main__':
    
    ### Déclaration Variables ###
    
    script_start_time = time.time()
    
    START_TIME = time.time()
    SESSION_ID = str(random.random())[2:8]
    
    LOG_PATH = '/sansto/logs/fos/supshow_logs'
    INPUT_PATH = '/sansto/tmp/fos/supshow_collect'
    OUTPUT_PATH = '/sansto/tmp/fos/supshow_parsing'
    
    LOG_FILE = LOG_PATH + '/supshow_parse_zone.log'
    
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    
    ### Gestion des arguments ###
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-d', '--debug', action="store_true", help='Debug Mode')
    parser.add_argument('-i', '--input_path', help='Select Input Path', default=INPUT_PATH)
    parser.add_argument('-o', '--output_path', help='Select Output Path', default=OUTPUT_PATH)
    parser.add_argument('-t', '--thread', help='Number of Thread Process [Default: 12]', type=int, default=12)
    parser.add_argument('-n', '--null_value', help='Value for empty fields [Default: '']', default='')
    
    args = parser.parse_args()
    
    thread_count = args.thread
    debug_mode = args.debug   
    null_val = args.null_value
    
    if debug_mode and args.output_path == OUTPUT_PATH:
        INPUT_PATH = '{0}/_Collect_Test/{1}'.format(CURRENT_DIR, time.strftime("%Y_%m_%d"))    
        OUTPUT_PATH = '{0}/_Parsing_Test/{1}'.format(CURRENT_DIR, time.strftime("%Y_%m_%d"))    
        
    else:    
        INPUT_PATH = '{0}/{1}'.format(args.input_path, time.strftime("%Y_%m_%d"))    
        OUTPUT_PATH = '{0}/{1}'.format(args.output_path, time.strftime("%Y_%m_%d"))    
    
    WWN_CSV_FILE = OUTPUT_PATH + '/supshow_parse_wwn_{0}.csv'.format(time.strftime("%Y_%m_%d"))
    PRI_CSV_FILE = OUTPUT_PATH + '/supshow_parse_pri_{0}.csv'.format(time.strftime("%Y_%m_%d"))
    
    ### Initialisation du Logger ###
    
    LOGGER = logging.getLogger()
    LOGGER.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s {0} %(levelname)s : %(message)s'.format(SESSION_ID), "%Y/%m/%d %H:%M:%S")
    
    if debug_mode:
        steam_handler = logging.StreamHandler()
        steam_handler.setLevel(logging.DEBUG)
        steam_handler.setFormatter(formatter)
        
        LOGGER.addHandler(steam_handler)
        
    else:
        file_handler = RotatingFileHandler(LOG_FILE, 'a', 1000000, 100)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        
        LOGGER.addHandler(file_handler)
        
    ### Démarrage du Script ###
    
    if not debug_mode:
        print 'Script Start (See Logs -> tail -f {0} | grep -w {1})'.format(LOG_FILE, SESSION_ID)
    
    LOGGER.info('--------->>>>>>>>>> SCRIPT START [THREAD:{0}][SESSION:{1}] <<<<<<<<<<---------'.format(thread_count, SESSION_ID))
    
    ### Vérification existance Input Path/File ###
    
    for path in [OUTPUT_PATH, INPUT_PATH]:
        if not os.path.exists(path):
            LOGGER.error('No Path Find {0} ! exit()'.format(path))
            sys.exit(1)
    
    for file in [WWN_CSV_FILE, PRI_CSV_FILE]:
        if not os.path.isfile(file):
            LOGGER.error('File : {0} Not Find ! exit()'.format(file))
            sys.exit(1)
        
    ### Déclaration des fichiers Output ###
    
    csv_file_dic_lst = [
        {
            'type': 'ZONE',
            'file': '{0}/supshow_parse_{1}_{2}.csv'.format(OUTPUT_PATH, 'zone', time.strftime("%Y_%m_%d")),
            'header': ['Zoneset', 'Zone', 'WWN', 'Alias', 'Chassis', 'Port', 'Date'],
        },
        {
            'type': 'ANOMALY',
            'file': '{0}/supshow_parse_{1}_{2}.csv'.format(OUTPUT_PATH, 'zone_anomaly', time.strftime("%Y_%m_%d")),
            'header': ['Master_Chassis', 'Master_FID', 'Master_Name', 'Zoneset', 'Zone', 'WWN', 'Alias', 'Anomalies', 'Switch_Name', 'Index', 'Date'],
        },
        {
            'type': 'ALIAS',
            'file': '{0}/supshow_parse_{1}_{2}.csv'.format(OUTPUT_PATH, 'alias_lst', time.strftime("%Y_%m_%d")),
            'header': ['Master_Chassis', 'Master_FID', 'Master_Name', 'Alias', 'WWN'],
        },
    ]
    
    ### Création des Headers et des fichiers CSV en Output (Write) ###
    
    for csv_file_dic in csv_file_dic_lst:
        header_maker(csv_file_dic['file'], csv_file_dic['header'])
    
    ### Récupération de la liste des fichiers du repertoire d'inuput (supportshow) ###
    
    supshow_file_all_lst = [l for l in os.listdir(INPUT_PATH) if re.search('^supportshow\..*\.out$', l)]
    
    if debug_mode:
        master_sw_dic = {}
        
        for s in supshow_file_all_lst:
            master_sw_dic[s.split('.')[1]] = [str(i) for i in range(1, 255)] + [null_val]
        
    else:
        master_sw_dic = get_master_sw(PRI_CSV_FILE)
        supshow_file_all_lst = [l for l in supshow_file_all_lst if l.split('.')[1] in master_sw_dic.keys()]
    
    if not supshow_file_all_lst:
        LOGGER.error('No Switch Find ! exit()')
        sys.exit(1)
    
    ### Mise dans un dictionnaire du Fichier d'output WWN ###
    
    wwn_csv_dic = {}
    
    with open(WWN_CSV_FILE, "r") as csvfile :
        csvlines = csv.reader(csvfile, delimiter=';')
        
        for v in csvlines:
            wwn_csv_dic[v[13]] = (v[2], v[6])
    
    ### Gestion des files de switch en fonction des threads ###
    
    sw_by_thread = int(len(supshow_file_all_lst)/thread_count)
    
    if sw_by_thread is 0:
        sw_by_thread += 1
    
    ### Generation d'une liste de liste de switch en fonction de la variable 'sw_by_thread' ###
    
    supshow_file_splt_lst = [supshow_file_all_lst[i:i + sw_by_thread] for i in xrange(0, len(supshow_file_all_lst), sw_by_thread)]
    
    LOGGER.info('Number of Switch  : {0}'.format(len(supshow_file_all_lst)))
    LOGGER.info('Number of Queue   : {0}'.format(len(supshow_file_splt_lst)))
    
    ### Lancement de la collecte en Multithread ###
    
    LOGGER.info('> PARSING ZONE START [{0}] <'.format(time.strftime("%H:%M:%S")))
    
    proc_lst = []
    
    for supshow_file_lst in supshow_file_splt_lst:
        
        lock = mp.Lock()
        proc = mp.Process(
            target=supshow_parser_manage,
            args = (
                LOGGER,
                INPUT_PATH,
                lock,
                supshow_file_lst,
                csv_file_dic_lst,
                master_sw_dic,
                wwn_csv_dic,
                null_val,
                debug_mode,
            )
        )
        
        proc_lst.append(proc)
        proc.start()
        
    try:
        for proc in proc_lst:
            proc.join()
        
    except KeyboardInterrupt:
        LOGGER.warning("SIGINT Signal Received")
        LOGGER.info("Terminate Process : {0}".format(','.join([p.name for p in proc_lst])))
        
        for proc in proc_lst:
            proc.terminate()
        
        
    script_execution_time = time.strftime("%H:%M:%S", time.gmtime((time.time() - script_start_time)))
    
    LOGGER.info('--------->>>>>>>>>> SCRIPT END [SESSION:{0}][T:{1}] <<<<<<<<<<---------'.format(SESSION_ID, script_execution_time))
    
    if not debug_mode:
        print 'Script End [{0}]'.format(script_execution_time)
