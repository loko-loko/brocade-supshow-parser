#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import re
import csv
import time
import pprint
import random
import logging
import shutil
import gzip
import tarfile
import argparse
import subprocess

from logging.handlers import RotatingFileHandler

def main():
    
    ### Déclaration Variables ###
    
    DATE_FMT = time.strftime("%Y_%m_%d")
    
    START_TIME = time.time()
    SESSION_ID = str(random.random())[2:8]
    
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    
    LOG_PATH = '/sansto/logs/fos/supshow_logs'
    LOG_FILE = LOG_PATH + '/supshow_fmt_clean.log'
    
    COLLECT_PATH = '/sansto/tmp/fos/supshow_collect/'
    PARSING_PATH = '/sansto/tmp/fos/supshow_parsing/'
    FINAL_PATH = '/sansto/output/fos'
    
    ARCHIVE_PARSING_PATH = '/sansto/arch/fos/supshow_parsing/'
    ARCHIVE_COLLECT_PATH = '/sansto/arch/fos/supshow_collect/'
    
    ### Gestion des arguments ###
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-d', '--debug', action="store_true", help='Debug Mode')
    parser.add_argument('-c', '--collect_path', help='Select Collect Path', default=COLLECT_PATH)
    parser.add_argument('-p', '--parsing_path', help='Select Parsing Path', default=PARSING_PATH)
    parser.add_argument('-f', '--final_path', help='Select Final Path', default=FINAL_PATH)
    parser.add_argument('-n', '--null_value', help='Value for empty fields [Default: '']', default='')
    
    args = parser.parse_args()
    
    debug_mode = args.debug   
    null_val = args.null_value
    
    if debug_mode and args.parsing_path == PARSING_PATH:
        COLLECT_PATH = '{0}/_Collect_Test/{1}'.format(CURRENT_DIR, DATE_FMT)    
        PARSING_PATH = '{0}/_Parsing_Test/{1}'.format(CURRENT_DIR, DATE_FMT)    
        FINAL_PATH = '{0}/Final'.format(PARSING_PATH)
        
    else:    
        COLLECT_PATH = '{0}/{1}'.format(args.collect_path, DATE_FMT)    
        PARSING_PATH = '{0}/{1}'.format(args.parsing_path, DATE_FMT)    
    
    WWN_CSV_FILE = PARSING_PATH + '/supshow_parse_wwn_{0}.csv'.format(DATE_FMT)
    
    CSV_FILE_DIC = {
        'listeZone' : '{0}/supshow_parse_zone_{1}.csv'.format(PARSING_PATH, DATE_FMT),
        'listeAlias' : '{0}/supshow_parse_alias_lst_{1}.csv'.format(PARSING_PATH, DATE_FMT),
        'listeSlot' : '{0}/supshow_parse_slot_{1}.csv'.format(PARSING_PATH, DATE_FMT),
        'listeSFP' : '{0}/supshow_parse_sfp_{1}.csv'.format(PARSING_PATH, DATE_FMT),
        'listeSwitchPr' : '{0}/supshow_parse_pri_{1}.csv'.format(PARSING_PATH, DATE_FMT),
        'listeOlsLrErreurs' : '{0}/supshow_parse_olslr_{1}.csv'.format(PARSING_PATH, DATE_FMT),
        'listeHardware' : '{0}/supshow_parse_hrdw_{1}.csv'.format(PARSING_PATH, DATE_FMT),
        'listeCP' : '{0}/supshow_parse_cp_{1}.csv'.format(PARSING_PATH, DATE_FMT),
        'listeSwitchInfos' : '{0}/supshow_parse_sw_info_{1}.csv'.format(PARSING_PATH, DATE_FMT),
        'listeAnomalieZone' : '{0}/supshow_parse_zone_anomaly_{1}.csv'.format(PARSING_PATH, DATE_FMT),
        'listeWWN' : '{0}/supshow_parse_fmt_wwn_{1}.csv'.format(PARSING_PATH, DATE_FMT),
    }
    
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
     
    print 'Script Start (See Logs -> tail -f {0} | grep -w {1})'.format(LOG_FILE, SESSION_ID)
    
    LOGGER.info('--------->>>>>>>>>> SCRIPT START [SESSION:{0}] <<<<<<<<<<---------'.format(SESSION_ID))
    
    ### Vérification existance Input Path/File ###
    
    for path in [COLLECT_PATH, PARSING_PATH]:
        if not os.path.exists(path):
            LOGGER.error('No Path Find {0} ! exit()'.format(path))
            sys.exit(1)
    
    for file in [WWN_CSV_FILE, CSV_FILE_DIC['listeAlias'], CSV_FILE_DIC['listeZone'], CSV_FILE_DIC['listeSwitchInfos']]:
        if not os.path.isfile(file):
            LOGGER.error('File : {0} Not Find ! exit()'.format(file))
            sys.exit(1)
        
    ### Création du Répertoire 'FINAL_PATH' ###
    
    LOGGER.info('Final Output Path : {0}'.format(FINAL_PATH))
    
    if not os.path.exists(FINAL_PATH):
        LOGGER.warning('Path : {0} Not Exist ! Script Create it'.format(FINAL_PATH))
        
        try:
            os.makedirs(FINAL_PATH)
            LOGGER.info('Create {0} [done]'.format(FINAL_PATH))
            
        except:
            LOGGER.error('Creation {0} Problem ! exit()'.format(FINAL_PATH))
            sys.exit(1)
        
    ### Modification du fichier WWN ###
    
    LOGGER.info('Modify WWN File')
    
    with open(WWN_CSV_FILE, "r") as csvfile:
        wwn_file_lst = [c for c in csv.reader(csvfile, delimiter=';')]
        
    with open(CSV_FILE_DIC['listeAlias'], "r") as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=';')
        
        wwn_alias_dic = {}
        
        for c in csv_reader:
            try:
                wwn_alias_dic[c[4]] = c[3]
            except:
                print c
        
    with open(CSV_FILE_DIC['listeWWN'], "w") as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=';')
        
        for l in wwn_file_lst:
            
            try:
                if l[0] != 'Meta_San':
                    l[14] = wwn_alias_dic.get(l[13], null_val)
                
                csv_writer.writerow(l)
            
            except IndexError:
                LOGGER.error('Output WWN File Line ({0}) Error -> {1}'.format(c, l))
            
    ### Modification du fichier Zone [Anomalie] ###
    
    LOGGER.info('Check Zone File')
    
    new_zone_file_lst = []
    zone_line_error_ck = 0
    
    with open(CSV_FILE_DIC['listeZone'], "rU") as csvfile:
        zone_file_lst = [c for c in csv.reader(csvfile, delimiter=';')]
        
    for c, z in enumerate(zone_file_lst):
        if len(z) is 7:
            new_zone_file_lst.append(z)
        else:
            LOGGER.error('Output Zone File Line ({0}) Error -> {1}'.format(c, z))
            zone_line_error_ck = 1
            
    if zone_line_error_ck is 1:
        print '<!> Output Zone File Line Error ! See Logs'
        
        LOGGER.info('Write New Zone File with no Error')
        
        with open(CSV_FILE_DIC['listeZone'], "w") as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=';')
            
            for z in new_zone_file_lst:
                csv_writer.writerow(z)
    
    ### Convertion des fichiers au format UNIX (dos2unix) ###
    
    LOGGER.info('Convert Files on Unix Format (dos2unix)')
    
    for new_name, file_path in CSV_FILE_DIC.items():
        subprocess.call(['dos2unix', file_path])
    
    ### Copie des fichiers du jours dans l'Output FOS ###
    
    LOGGER.info('Files Copy (WWN, Zone) in {0}'.format(FINAL_PATH))
    
    for new_name, file_path in CSV_FILE_DIC.items():
        shutil.copy2(file_path, '{0}/{1}.csv'.format(FINAL_PATH, new_name))
    
    if DATE_FMT == time.strftime("%Y_%m_%d") and not debug_mode: 
    
        ### Compression des CSV SupportShow ###
        
        for new_name, file_path in CSV_FILE_DIC.items():
            with open(file_path, 'rb') as f_in:
                f_out = gzip.open('{0}/SupShow_{1}_{2}.csv.gz'.format(ARCHIVE_PARSING_PATH, new_name, DATE_FMT), 'wb')
                shutil.copyfileobj(f_in, f_out)
    
        ### Compression et Archivage des Fichiers SupportShow ###
    
        LOGGER.info('Compression Collect File Start')
        
        supshow_tar_file = '{0}/{1}_supshow_files.tar'.format(ARCHIVE_COLLECT_PATH, DATE_FMT)
        
        tar = tarfile.open(supshow_tar_file, "w")
        
        for file in os.listdir(COLLECT_PATH):
            LOGGER.info('[tar] Add SupportShow {0} to Archive ..'.format(file))
            tar.add(COLLECT_PATH + '/' + file)
            
        tar.close()
        
        LOGGER.info('[xz] Compression Tar File {0} ..'.format(supshow_tar_file))
        os.system('xz -zkf9 {0}'.format(supshow_tar_file))
        
        LOGGER.info('Delete Tar File {0} ..'.format(supshow_tar_file))
        os.remove(supshow_tar_file)
        
        ### Suppression des SupportShow [Fichiers + CSV] ###
        
        LOGGER.info('Delete SupportShow Collect Temporary Dir : {0}'.format(COLLECT_PATH))
        shutil.rmtree(COLLECT_PATH)
        
        LOGGER.info('Delete SupportShow Parsing Temporary Dir : {0}'.format(PARSING_PATH))
        shutil.rmtree(PARSING_PATH)
    
    script_execution_time = time.strftime("%H:%M:%S", time.gmtime((time.time() - START_TIME)))
    LOGGER.info('--------->>>>>>>>>> SCRIPT END [SESSION:{0}][T:{1}] <<<<<<<<<<---------'.format(SESSION_ID, script_execution_time))
    print 'Script End [{0}]'.format(script_execution_time)

if __name__ == '__main__':
    main()

