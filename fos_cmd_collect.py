#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import sys
import os
import re
import time
import pprint
import random    
import logging
import shutil
import argparse
import signal

import paramiko
import multiprocessing as mp
from logging.handlers import RotatingFileHandler
from operator import itemgetter
    
    ### Fonctions Globales ###

def connect(logger, user, sw, ssh_timeout=120):
    """ Connection SSH (Module Paramiko) """
    
    logger.info('SSH Connect on {0}'.format(sw))
    
    try:
        conn = paramiko.SSHClient()
        conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        conn.connect(sw, username=user, timeout=ssh_timeout)
        
    except:
        logger.error('SSH Connect on {0} Problem'.format(sw))
        return 1
        
    return conn
    
def cmd_exec(logger, conn, sw, cmd, fid=False, cmd_timeout=300):
    
    if fid:
        cmd = 'fosexec --fid {0} -cmd "{1}"'.format(fid, cmd)
        
    logger.info('command <{0}> execution on {1}'.format(cmd, sw))
    
    try:
        stdin, stdout, stderr = conn.exec_command(cmd)        
        stdout.channel.settimeout(cmd_timeout)        
        stdin.close()        
        
    except:        
        logger.error('Command [{0}] execution Timeout on {1}'.format(cmd, sw))
        return 1     

    return stdout.read()
    
    
def get_fid_lst(logger, conn, sw):
    
    vf_cmd_out = cmd_exec(logger, conn, sw, 'lscfg --show').split('\n')
    
    fid_lst = []
    
    try:
        for vf_cmd in vf_cmd_out:
            if 'Domain IDs' in vf_cmd:
                fid_lst = [v.split('(')[0] for v in vf_cmd.split() if re.search('^[0-9]+\(', v)]
    
    except IndexError:
        return 1
    
    if fid_lst:
        return fid_lst
        
    return 1
    
    
def format_cmd(command_lst, vf_command_lst):
    
    command_dic = {}
    
    command_dic['no_vf'] = False
    command_dic['vf'] = False
    
    if command_lst:
        command_dic['no_vf'] = list(set(command_lst))
        
    if vf_command_lst:
        command_dic['vf'] = list(set(vf_command_lst))  
    
    return command_dic
    
def format_sw_list(logger, sw_lst, except_sw_lst):
    """ Création de file de switchs en fonction de leurs nomenclatures par Localisation et MetaSAN [Nomenclature switch BP2i] """
    
    result = []
    
    logger.info('Format Switch List')
    
    for sw in sw_lst:
        
        sw_dic = {}
        
        sw_dic['sw_name'] = sw
        sw_dic['sw_msan'] = sw_dic['sw_name'][1]
        
        sw_dic['sw_type'] = 'odd'
        
        if int(sw_dic['sw_name'][-1]) % 2 != 0:
            sw_dic['sw_type'] = 'even'
        
        if sw_dic['sw_msan'] == 'w':
            sw_dic['sw_msan'] = '1'
        
        if sw_dic['sw_msan'] == 'b':
            sw_dic['sw_msan'] = sw_dic['sw_name'][0:4].upper()
            sw_dic['sw_loc'] = sw_dic['sw_name'][4:6].upper()
            
        else:
            sw_dic['sw_loc'] = sw_dic['sw_name'][5:7].upper()
            
            if len(sw_dic['sw_name']) != 10 and re.search('(N|S)0', sw_dic['sw_loc']):
                sw_dic['sw_loc'] = sw_dic['sw_name'][4:6].upper()
            
        if re.search('^(M|V|Y)(A|N|E|[0-9])', sw_dic['sw_loc']):
            
            if 'V' in sw_dic['sw_loc']:
                sw_dic['sw_loc'] = 'VA'
                
            sw_dic['sw_msan'] = 'FR_' + sw_dic['sw_msan'] + '_' + sw_dic['sw_loc']
                
            
        elif re.search('^B(N|S)', sw_dic['sw_loc']):
            sw_dic['sw_msan'] = 'BE_' + sw_dic['sw_msan'] + '_' + sw_dic['sw_loc']
        
        elif re.search('^PM|AD', sw_dic['sw_loc']):
            
            if re.search('sw51', sw_dic['sw_name']):
                sw_dic['sw_msan'] = 'SAV'
                
            sw_dic['sw_msan'] = 'IT_' + sw_dic['sw_msan']
            
        
        ### Gestion des Exceptions ###
        
        if re.search('_[A-Z]B0[1-9]+_', sw_dic['sw_msan']):
            sw_dic['sw_msan'] = sw_dic['sw_msan'].split('_')[0] + '_' + sw_dic['sw_msan'].split('_')[1]
        
        elif re.search('FR_1_M1', sw_dic['sw_msan']):
            sw_dic['sw_msan'] = 'FR_1_M2'
            
        elif re.search('^FR_4_', sw_dic['sw_msan']):
            sw_dic['sw_msan'] = 'FR_4_ALL'
            
        elif re.search('FR_2_M8', sw_dic['sw_msan']):
            sw_dic['sw_msan'] = 'FR_2_M7'
            
        elif re.search('FR_[2-3]_VA', sw_dic['sw_msan']):
            sw_dic['sw_msan'] = 'FR_23_VA'
         
        elif re.search('FR_1_Y', sw_dic['sw_msan']):
            sw_dic['sw_msan'] = 'FR_1_Y13'
        
        elif re.search('FR_3_M[5-6]', sw_dic['sw_msan']):
            sw_dic['sw_msan'] = 'FR_3_M56'
        
        elif sw_dic['sw_name'] in except_sw_lst:
            sw_dic['sw_msan'] = 'EXCEPT_LST'
            
        result.append(sw_dic)
    
    return result
    
def generate_queue(logger, sw_dic_lst):
    """ Generation des files de switch par type """
    
    result = []
    
    logger.info('Generating Queue')
    
    msan_lst = list(set([s['sw_msan'] for s in sw_dic_lst]))
    
    for msan in msan_lst:
        
        dic = {}
        
        dic['sw_msan'] = msan
        dic['sw_lst'] = sorted([s for s in sw_dic_lst if s['sw_msan'] == msan], key=itemgetter('sw_type'))
        
        result.append(dic)
        
    return result

    
def sw_collect_exec(logger, output_path, user, sw, command_dic):
    """ Collecte des commandes sur les switchs """
    
    ### timeout du retour de la commande ###
    
    command_timeout = 2700 # 45 min
    conn = connect(logger, user, sw)
    
    if conn is 1:
        logger.error('collect Abord for switch {0}'.format(sw))
        return 1
        
    ### Liste des commandes à executer ###
    
    for cmd_type, cmd_lst in command_dic.items():
        
        if cmd_lst:
            
            if cmd_type == 'vf':
                
                fid_lst = get_fid_lst(logger, conn, sw)
                
                if fid_lst is 1:
                    logger.warning('No VF Find on {0} !'.format(sw))
                    cmd_type = 'no_vf'
                    
                else:
                    
                    for fid in fid_lst:
                        
                        for cmd in cmd_lst:
                        
                            ### Génération de l'output (ex: command.swdxem6070.128.2018_01_05.out) ###
                            
                            output_file = '{0}/{1}.{2}.{3}.{4}.out'.format(output_path, cmd.replace(' ', '_'), sw.lower(), fid, time.strftime("%Y_%m_%d"))
                            
                            cmd_out = cmd_exec(logger, conn, sw, cmd, fid, command_timeout)
                            
                            if cmd_out is not 1:
                            
                                ### Ecriture des output ###
                                
                                logger.info('write return cmd for switch {0} on file {1}'.format(sw, output_file))
                                    
                                with open(output_file, 'w') as f:
                                    f.write(cmd_out)
                                
                            else:
                                os.remove(output_file)
                                return cmd_out
                    
                
            if cmd_type == 'no_vf':
            
                for cmd in cmd_lst:
            
                    ### Génération de l'output (ex: command.swdxem6070.2018_01_05.out) ###
                    
                    output_file = '{0}/{1}.{2}.{3}.out'.format(output_path, cmd.replace(' ', '_').replace('-', ''), sw.lower(), time.strftime("%Y_%m_%d"))
                    
                    ### Lancement des commandes ###
                    
                    cmd_out = cmd_exec(logger, conn, sw, cmd)
                    
                    if cmd_out is not 1:
                    
                        ### Ecriture des output ###
                        
                        logger.info('write return cmd for switch {0} on file {1}'.format(sw, output_file))
                            
                        with open(output_file, 'w') as f:
                            f.write(cmd_out)
                        
                    else:
                        os.remove(output_file)
                        
                        return cmd_out
            
    return True
    
def sw_collect_manage(logger, output_path, user, command_dic, sw_loc):
    
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    
    pid = os.getpid()
    name = mp.current_process().name
    
    sw_supshow_failed_lst = []
    
    logger.info('PID:{0}, M.SAN:{1}, P.NAME:{2}, SWITCH COUNT:{3} [start]'.format(pid, sw_loc['sw_msan'], name, len(sw_loc['sw_lst'])))
    
    time.sleep(.5)
    
    for sw in sw_loc['sw_lst']:
        
        process_start_time = time.time()
        
        logger.info('P:{0}, M:{1}, L:{2}, S:{3} [start]'.format(pid, sw['sw_msan'], sw['sw_loc'], sw['sw_name']))
        
        return_collect = sw_collect_exec(logger, output_path, user, sw['sw_name'], command_dic)
        
        process_end_time = time.strftime("%H:%M:%S", time.gmtime((time.time() - process_start_time)))
        
        if return_collect is 1: 
            logger.error('collect error for switch {0} [T:{1}]'.format(sw['sw_name'], process_end_time))
            
        elif return_collect is 2:
            logger.warning('supportshow collect failed on {0}, Retry after ! Pass [{1}]'.format(sw['sw_name'], process_end_time))
            sw_supshow_failed_lst.append(sw['sw_name'])
            
        else: 
            logger.info('collect done for switch {0} [T:{1}]'.format(sw['sw_name'], process_end_time))
            
    # if sw_supshow_failed_lst:
        
        # max_retry_count = 3
        
        # while sw_supshow_failed_lst:
            
            # for sw in sw_supshow_failed_lst:
                
                # process_start_time = time.time()
                
                # logger.info('Pid:{0}, Switch:{1} [Retry:{2}] [start]'.format(pid, sw, retry_count))
                
                # return_collect = sw_collect_exec(logger, output_path, user, sw, command_dic)
                
                # process_end_time = time.strftime("%H:%M:%S", time.gmtime((time.time() - process_start_time)))
                
                # if return_collect is 1: 
                    # logger.error('collect error for switch {0} [T:{1}]'.format(sw, process_end_time))
                    # sw_supshow_failed_lst.remove(sw)
                    
                # elif return_collect is 2:
                    # logger.warning('Retry failed on {0}, Retry after ! Pass [{1}]'.format(sw, process_end_time))
                    # retry_count += 1
                    
                # else: 
                    # logger.info('collect done for switch {0} [T:{1}]'.format(sw, process_end_time))
                    # sw_supshow_failed_lst.remove(sw)
                    
                
    logger.info('PID:{0}, M.SAN:{1}, P.NAME:{2}, SWITCH COUNT:{3} [terminate]'.format(pid, sw_loc['sw_msan'], name, len(sw_loc['sw_lst'])))
    
    
    ### Corp ###
    
if __name__ == '__main__':
    
    ### Variables Global ###
    
    script_start_time = time.time()
    
    USER = 'newscript'
    
    START_TIME = time.time()
    SESSION_ID = str(random.random())[2:8]
    
    ETC_PATH = '/sansto/etc/fos'
    LOG_PATH = '/sansto/logs/fos/supshow_logs'
    OUTPUT_PATH = '/sansto/tmp/fos/supshow_collect'
    
    LOG_FILE = '{0}/fos_cmd_collect.log'.format(LOG_PATH)
    ETC_FILE = '{0}/collect_sw_list.txt'.format(ETC_PATH)
    
    
    
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    
    ### Gestion des arguments ###
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-d', '--debug', action="store_true", help='Debug Mode')
    parser.add_argument('-c', '--command', help='Command to Collect')
    parser.add_argument('-v', '--vf_command', help='VF Command to Collect')
    parser.add_argument('-s', '--switch', help='Switchs List')
    parser.add_argument('-o', '--output_path', help='Select Output Path', default=OUTPUT_PATH)
    
    args = parser.parse_args()
    
    debug_mode = args.debug
    sw_lst_arg = args.switch
    
    command_lst = []
    vf_command_lst = []
    
    if args.command is None and args.vf_command is None:
        print '<!> Command Necessary ! Type -h'
        exit(1)
        
    if args.command:
        command_lst = args.command.split(',')
        
    if args.vf_command:
        vf_command_lst = args.vf_command.split(',')
    
    if debug_mode and args.output_path == OUTPUT_PATH:
        OUTPUT_PATH = '{0}/_Collect_Test/{1}'.format(CURRENT_DIR, time.strftime("%Y_%m_%d"))
    
    else:
        OUTPUT_PATH = '{0}/{1}'.format(args.output_path, time.strftime("%Y_%m_%d"))
    
    command_dic = format_cmd(command_lst, vf_command_lst)
    
    ### Listes des switchs à exclure de la collecte ###
    
    EXCEPT_SW_LST = []
    
    ### Initialisation du Logger ###
    
    LOGGER = logging.getLogger()
    LOGGER.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s {0} %(levelname)s > %(message)s'.format(SESSION_ID), "%Y/%m/%d %H:%M:%S")
    
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
    
    LOGGER.info('--------->>>>>>>>>> SCRIPT START [SESSION:{0}] <<<<<<<<<<---------'.format(SESSION_ID))
    
    ### Création[Suppression] du Répertoire 'OUTPUT_PATH' ###
    
    LOGGER.info('Output Path : {0}'.format(OUTPUT_PATH))
    
    if os.path.exists(OUTPUT_PATH):
        LOGGER.warning('Path Already Exist !. Script Delete Dir'.format(OUTPUT_PATH))
        
        try:
            shutil.rmtree(OUTPUT_PATH)
            LOGGER.info('Delete {0} [Done]'.format(OUTPUT_PATH))
        except:
            LOGGER.error('Delete {0} [Problem]'.format(OUTPUT_PATH))
            exit(1)
        
    
    try:
        os.makedirs(OUTPUT_PATH)
        LOGGER.info('Create {0} [Done]'.format(OUTPUT_PATH))
    except:
        LOGGER.error('Creation {0} [Problem]'.format(OUTPUT_PATH))
        exit(1)
        
        
    ### Formatage des switchs par Site et Queue ###
    
    if sw_lst_arg:
        input_sw_lst = sw_lst_arg.split(',')
    
    else:
        with open(ETC_FILE, "r") as csvfile :
            input_sw_lst = [c[0] for c in csv.reader(csvfile, delimiter=';') if not re.search('^\s*(#|$)', c[0])]
            
            
    sw_dic_lst = format_sw_list(LOGGER, input_sw_lst, EXCEPT_SW_LST)
    sw_loc_lst = generate_queue(LOGGER, sw_dic_lst)
    
    msan_lst = list(set([s['sw_msan'] for s in sw_dic_lst]))
    
    ### Affichage des files de switchs ###
    
    LOGGER.info('{0} Switchs - Generating of {1} Queue'.format(len(sw_dic_lst), len(sw_loc_lst)))
    
    for sw_loc in sw_loc_lst:
        LOGGER.info('{0} SW -> {1:<12} : {2}'.format(str(len(sw_loc['sw_lst'])).zfill(2), sw_loc['sw_msan'], ','.join([s['sw_name'] for s in sw_loc['sw_lst']])))
    
    ### Lancement de la collecte en Multithread ###
    
    LOGGER.info('> COLLECT START [{0}] <'.format(time.strftime("%H:%M:%S")))
    
    proc_lst = []
    
    for sw_loc in sw_loc_lst:
        
        proc = mp.Process(target=sw_collect_manage, args=(LOGGER, OUTPUT_PATH, USER, command_dic, sw_loc))
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
    
    
