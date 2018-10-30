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
    
    
def get_slot_info(sw_dic, slotshow_lst, chassisshow_lst, null_val):
    """ Récupération des infos sur les slots """
    
    slot_csv_lst = []
    
    for slotshow in slotshow_lst:
        
        if re.search('\s+[1-9]+\s+', slotshow):
            
            slot_info = slotshow.split()
            
            slot_dic = {}
            
            slot_dic['id'] = null_val
            slot_dic['model'] = null_val
            slot_dic['part_num'] = null_val
            slot_dic['serial_num'] = null_val
            
            slot_dic['slot'] = slot_info[0]
            slot_dic['type'] = slot_info[1]
            
            if slot_dic['type'] != 'UNKNOWN':
                
                slot_dic['id'] = slot_info[3]
                slot_dic['model'] = slot_info[4]
                
                slot_chassisshow_lst = get_line_cmd(chassisshow_lst, r'^{0}\s+BLADE\s+Slot:\s+{1}'.format(slot_dic['type'], slot_dic['slot']), r'^Time Awake:')
                
                slot_dic['part_num'] = [s.split()[-1] for s in slot_chassisshow_lst if re.search('^Factory Part Num', s)][0]
                
                if slot_dic['part_num'] == 'num:':
                    slot_dic['part_num'] = null_val
                
                slot_dic['serial_num'] = [s.split()[-1] for s in slot_chassisshow_lst if re.search('^Factory Serial Num', s)][0]
                
            else:
                slot_dic['type'] = null_val
                
            slot_dic['status'] = slot_info[-1].lower()
            
            slot_csv_lst.append([
                sw_dic['name'],
                sw_dic['sw_serial'],
                slot_dic['slot'],
                slot_dic['type'],
                slot_dic['id'],
                slot_dic['model'],
                slot_dic['status'],
                slot_dic['serial_num'],
                slot_dic['part_num'],
            ])
            
    return slot_csv_lst  

    
def get_hrdw_info(sw_dic, chassisshow_lst, null_val):
    """ Récupération des infos sur le Hardware """
    
    hrdw_csv_lst = []
    
    hw_lst = [l for l in chassisshow_lst if re.search(r'^(POWER|FAN|WWN|CHASSIS/WWN)', l)]
    
    for hw in hw_lst:
        
        hw_dic = {}
        
        if 'POWER' in hw:
            rgx = r'^{0}'.format(hw)
            hw_dic['hw_type'] = 'PWR'
            hw_dic['unit_id'] = hw.split()[-1]
        elif 'FAN' in hw:
            rgx = r'^{0}'.format(hw)
            hw_dic['hw_type'] = 'FAN'
            hw_dic['unit_id'] = hw.split()[-1]
        elif 'WWN' in hw:
            rgx = r'^{0}\s+{1}\s+{2}'.format(hw.split()[0], hw.split()[1], hw.split()[2])
            hw_dic['hw_type'] = 'WWN'
            hw_dic['unit_id'] = hw.split()[2]
            
        hw_chassisshow_lst = get_line_cmd(chassisshow_lst, rgx, r'^$')
        
        try:
            hw_dic['part_num'] = [h.split()[-1] for h in hw_chassisshow_lst if re.search('^Factory Part Num', h)][0]
            hw_dic['serial_num'] = [h.split()[-1] for h in hw_chassisshow_lst if re.search('^Factory Serial Num', h)][0]
        
        except IndexError:
            hw_dic['part_num'] = null_val
            hw_dic['serial_num'] = null_val
        
        hrdw_csv_lst.append([
            sw_dic['name'],
            sw_dic['sw_serial'],
            hw_dic['hw_type'],
            hw_dic['unit_id'],
            hw_dic['serial_num'],
            hw_dic['part_num'],  
        ])

    return hrdw_csv_lst
    
def get_cp_info(sw_dic, firmwareshow_lst, chassisshow_lst, null_val):
    """ Récupération des infos sur les CP """
    
    cp_csv_lst = []
    
    cp_lst = list(set([f.split()[1] for f in firmwareshow_lst if re.search(r'\s+[0-9]+\s+CP[0-1]', f)]))
    version_lst = []
    
    if cp_lst:
    
        for cp in cp_lst:
            
            cp_dic = {}
            
            cp_info_lst = [f.split() for f in firmwareshow_lst if re.search(r'\s+[0-9]+\s+{0}'.format(cp), f)][0]
            
            cp_dic['name'] = cp
            cp_dic['version'] = cp_info_lst[3]
            cp_dic['status'] = cp_info_lst[4].lower().replace(' *', '')
            
            cp_csv_lst.append([
                sw_dic['name'],
                sw_dic['sw_serial'],
                sw_dic['sw_factory_sn'],
                cp_dic['name'],
                cp_dic['version'],
                cp_dic['status'],
            ])
            
            version_lst.append(cp_dic['version'])
            
    else:
        
        cp_dic = {}
        
        cp_dic['name'] = null_val
        cp_dic['version'] = null_val
        cp_dic['status'] = null_val
        
        try:
            chassis_wwn_info_lst = get_line_cmd(chassisshow_lst, '^CHASSIS/WWN  Unit: 1', '^Serial Num:')
        except:
            chassis_wwn_info_lst = False
        
        if chassis_wwn_info_lst:
            sw_dic['sw_factory_sn'] = [c.split()[-1] for c in chassis_wwn_info_lst if re.search(r'^Factory Serial Num:', c)][0]
            cp_dic['name'] = [f.split()[0] for f in firmwareshow_lst if re.search(r'^FOS\s+v[0-9]+', f)][0]
            cp_dic['version'] = [f.split()[-1] for f in firmwareshow_lst if re.search(r'^FOS\s+v[0-9]+', f)][0]
        
        cp_csv_lst.append([
            sw_dic['name'],
            sw_dic['sw_serial'],
            sw_dic['sw_factory_sn'],
            cp_dic['name'],
            cp_dic['version'],
            cp_dic['status'],
        ])
        
        version_lst.append(cp_dic['version'])
        
    version_lst = list(set(version_lst))
        
    if not version_lst:
        version_lst = [null_val]
        
    return cp_csv_lst, version_lst
    
    
def get_switch_info(fid_dic, switchshow_fid_lst, cfgsize_fid_lst, null_val):
    """ Récupération des infos de la commande switchshow """
    
    for s in switchshow_fid_lst:
        if 'switchName:' in s:
            fid_dic['sw_name'] = s.split('\t')[-1]
        elif 'switchWwn:' in s:
            fid_dic['sw_wwn'] = s.split('\t')[-1]
        elif 'switchType:' in s:
            fid_dic['sw_type'] = s.split('\t')[-1]
        elif 'zoning:' in s:
            fid_dic['sw_zone_set'] = s.split('\t')[-1]
            
    if 'ON' in fid_dic['sw_zone_set']:
        fid_dic['sw_zone_set'] = re.sub('[\(\)]', '', fid_dic['sw_zone_set'].split()[-1])
    
    else:
        fid_dic['sw_zone_set'] = null_val
        
    fid_dic['db_size'] = [c.split()[-1] for c in cfgsize_fid_lst if 'committed -' in c][0]
        
    
def get_switchshow_info(port_dic, switchshow_fid_lst, no_slot, null_val):
    """ Récupération des infos de la commande switchshow """
    
    if no_slot:
        port_switchshow_info_lst = [p.split() for p in switchshow_fid_lst if re.search(r'\s*{0}\s+{1}'.format(port_dic['index'], port_dic['port']), p)][0]
        port_switchshow_info_lst.insert(1, '0')
        
    else:
        port_switchshow_info_lst = [p.split() for p in switchshow_fid_lst if re.search(r'\s*{0}\s+{1}\s+{2}'.format(port_dic['index'], port_dic['slot'], port_dic['port']), p)][0]
    
    port_switchshow_info_join = ' '.join(port_switchshow_info_lst)
    
    port_dic['trunk'] = null_val
    port_dic['master'] = null_val
    
    port_dic['npiv'] = False
    
    port_dic['speed'] = port_switchshow_info_lst[5]
    port_dic['state'] = port_switchshow_info_lst[6]
    
    type_lst = []
    
    if re.search(' (LS|LD) ', port_switchshow_info_join):
        type_lst.append(port_switchshow_info_lst[8])
        type_lst.append(port_switchshow_info_lst[9])
    
    elif '-Port' in port_switchshow_info_join:
        type_lst.append(port_switchshow_info_lst[8])
    
    if port_dic['state'] != 'Online':
        
        if 'Disabled' in port_switchshow_info_join:
            type_lst.append('Disabled')
            
        if 'Persistent' in port_switchshow_info_join:
            port_dic['wwn'] = 'Persistent'
        
    if '-Port' in ' '.join(type_lst) and not 'Disabled' in type_lst:
        
        try:
            port_dic['wwn'] = port_switchshow_info_lst[9]
        except IndexError:
            pass
        
        if not 'F-Port' in type_lst and 'Trunk' in port_switchshow_info_join:
            port_dic['wwn'] = 'Trunk'
            
        elif 'NPIV' in port_switchshow_info_join:
                port_dic['npiv'] = True
                type_lst = ['NPIV']
            
    if not type_lst:
        type_lst = [null_val]
        
    port_dic['type'] = ' '.join(type_lst)    
   
    
def get_sfpshow_info(port_dic, sfpshow_fid_lst, no_slot, null_val):
    """ Récupération info sur les SFP (cmd: sfpshow) """
    
    if no_slot:
        sfp_head_rgx = r'^Port\s+{1}:'.format(port_dic['slot'], port_dic['port'])
    else:
        sfp_head_rgx = r'^Slot\s+{0}/Port\s+{1}:'.format(port_dic['slot'], port_dic['port'])
                
        port_sfpshow_info_lst = get_line_cmd(sfpshow_fid_lst, sfp_head_rgx, r'Last poll time|SFP or is disabled|^$')
        
        sfp_rx_info = [p for p in port_sfpshow_info_lst if re.search(r'RX', p)]
        sfp_tx_info = [p for p in port_sfpshow_info_lst if re.search(r'TX', p)]
        
        if sfp_rx_info or sfp_tx_info:
            
            port_dic['sfp'] = 'yes'
            
            port_dic['sfp_connector'] = [p.split()[-1] for p in port_sfpshow_info_lst if re.search('^Connector:', p)][0]
            port_dic['sfp_serial_num'] = [p.split()[-1] for p in port_sfpshow_info_lst if re.search('^Serial No:', p)][0]
            port_dic['sfp_part_num'] = [p.split()[-1] for p in port_sfpshow_info_lst if re.search('^Vendor PN:', p)][0]
            port_dic['sfp_transceiver'] = [p.split()[-1] for p in port_sfpshow_info_lst if re.search('^Transceiver:', p)][0]
            port_dic['sfp_av_speed'] = [p.split()[2] for p in port_sfpshow_info_lst if re.search('^Transceiver:', p)][0]
            port_dic['sfp_lenght_9u_km'] = [p.split()[2] for p in port_sfpshow_info_lst if re.search('^Length 9u:', p)][0]
        
def get_portshow_info(port_dic, portshow_fid_lst, npiv, null_val):
    """ Récupération info sur les NPIV   """
    
    port_portshow_lst = get_line_cmd(portshow_fid_lst, r'^portshow {0}$'.format(port_dic['index']), r'^portrouteshow {0}$'.format(port_dic['index']))
    
    try:
        lr_ols_info = [p.split()[-1] for p in port_portshow_lst if re.search('(Lr|Ols)_(in|out)', p)]
        
        port_dic['lr_in'] = lr_ols_info[0]
        port_dic['lr_out'] = lr_ols_info[1]
        port_dic['ols_in'] = lr_ols_info[2]
        port_dic['ols_out'] = lr_ols_info[3]
        
        date_stats_clear = [w.split('\t')[1] for w in port_portshow_lst if re.search('^phy_stats_clear_ts', w)][0]
        port_dic['stats_clear'] = date_stats_clear.split()[0] + ' ' + date_stats_clear.split()[-1]
        
    except IndexError:
        port_dic['lr_in'] = null_val
        port_dic['lr_out'] = null_val
        port_dic['ols_in'] = null_val
        port_dic['ols_out'] = null_val
    
    if npiv:
        wwn_device_connect = [w.replace('\t', '') for w in port_portshow_lst if re.search('^\s+(c|[1-9])[0-9]:[0-9][0-9]:', w)]
        
        port_dic['wwn'] = [w for w in wwn_device_connect if not re.search('^c0', w)][0]
        port_dic['npiv_wwn_lst'] = [w for w in wwn_device_connect if re.search('^c0', w)]
        
        if not port_dic['npiv_wwn_lst']:
            port_dic['npiv_wwn_lst'].append(null_val)
        else:
            port_dic['npiv_wwn_lst'].append(port_dic['wwn'])
    
def get_portname(port_dic, port_name_lst, null_val):
    """ Récupération portname """
    
    try:
        port_dic['port_name'] = [p.split(':')[-1] for p in port_name_lst if re.search(r'^portCfgName.{0}:'.format(port_dic['index']), p)][0]
        
        if port_dic['port_name'] == '----':
            port_dic['port_name'] = null_val
        
    except IndexError:
        port_dic['port_name'] = null_val
    
def get_pri_info(fid_dic, fabricshow_fid_lst, null_val):
    """ Récupération Info Master Switch """
    
    fid_dic['is_master'] = 'No'
    
    try:
        fid_dic['sw_master'] = [f.split()[-1] for f in fabricshow_fid_lst if f != '' and re.search(r'>\".*\"', f.split()[-1])][0].replace('"', '').replace('>', '')
        fid_dic['domain_id'] = [f.split()[0] for f in fabricshow_fid_lst if f != '' and re.search(r'\"{0}\"'.format(fid_dic['sw_name']), f.split()[-1])][0].replace(':', '')
    
    except IndexError:
        fid_dic['sw_master'] = 'No_Fab'
        fid_dic['domain_id'] = null_val
    
    if fid_dic['sw_name'] == fid_dic['sw_master']:
        fid_dic['is_master'] = 'Yes'
    
    
def supshow_parser_exec(logger, input_path, lock, supshow_file, csv_file_dic_lst, null_val):
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
    
    for cmd in ['fid', 'port_name', 'sfpshow', 'switchshow', 'portshow', 'porterrshow', 'firmwareshow', 'slotshow', 'chassisshow', 'fabricshow', 'cfgsize']:
        exec(cmd + '_lst = []')
        exec(cmd + '_check = 0')
    
    for line in supshow_lst:
        
        if re.search(r'CURRENT CONTEXT -- [0-9]+', line):
            fid_lst.append(line.split()[-1])
        
        port_name_check = get_line_sup(port_name_lst, line, port_name_check, r'^\[Banner\]', r'^portEportCredits.')
        sfpshow_check = get_line_sup(sfpshow_lst, line, sfpshow_check, r'^sfpshow -all\s+:', r'^porterrshow\s+:')
        switchshow_check = get_line_sup(switchshow_lst, line, switchshow_check, r'^switchshow\s+:', r'^tempshow\s+:')
        portshow_check = get_line_sup(portshow_lst, line, portshow_check, r'^portshow [0-9]+', r'^Please run supportSave')
        porterrshow_check = get_line_sup(porterrshow_lst, line, porterrshow_check, r'^porterrshow\s+:', r'^(snmpdsupportshow|fwsamshow)\s+:')
        firmwareshow_check = get_line_sup(firmwareshow_lst, line, firmwareshow_check, r'^firmwareshow -v\s+:', r'^(firmwareshow --history|firmwaredownloadstatus)\s+:')
        slotshow_check = get_line_sup(slotshow_lst, line, slotshow_check, r'^slotshow -m\s+:', r'^slotshow -d576\s+:')
        chassisshow_check = get_line_sup(chassisshow_lst, line, chassisshow_check, r'^chassisshow\s+:', r'^timeout\s+:')
        fabricshow_check = get_line_sup(fabricshow_lst, line, fabricshow_check, r'^fabricshow\s+:', r'^(fabricshow -version|dom)\s+:')
        cfgsize_check = get_line_sup(cfgsize_lst, line, cfgsize_check, r'^cfgsize\s+:', r'^cfgshow\s+:')
        
    port_name_lst = [p for p in port_name_lst if re.search(r'^portCfgName.[0-9]+:', p)]
    
    del supshow_lst
    supshow.close()
        
    ### Récupération des informations générales du Switch ###
    
    try:
        sw_dic['sw_serial'] = [l.split()[-1] for l in chassisshow_lst if re.search(r'^Serial Num:', l)][0]
        
    except IndexError:
        sw_dic['sw_serial'] = None
        logger.info('SW:{0} SupportShow Problem. No Collect on It'.format(sw_dic['name']))
        
    try:
        sw_dic['sw_factory_sn'] = [l.split()[-1] for l in chassisshow_lst if re.search(r'^Chassis Factory Serial Num:', l)][0]
        
    except IndexError:
        sw_dic['sw_factory_sn'] = null_val
        
    if sw_dic['sw_serial'] is not None:
    
        sw_dic['fid_lst'] = list(set(fid_lst))
        
        get_location_info(sw_dic)
        
        if not sw_dic['fid_lst']:
            sw_dic['fid_lst'].append(null_val)
        
        ### Récupération infos Slot/Hardware/CP ###
        
        slot_csv_lst = get_slot_info(sw_dic, slotshow_lst, chassisshow_lst, null_val)
        hrdw_csv_lst = get_hrdw_info(sw_dic, chassisshow_lst, null_val)
        cp_csv_lst, sw_dic['version_lst'] = get_cp_info(sw_dic, firmwareshow_lst, chassisshow_lst, null_val)
        
        ### Récupération infos par FID ###
        
        for i, fid in enumerate(sw_dic['fid_lst']):
            
            fid_dic = {}
            
            fid_dic['fid'] = fid
            
            ### Découpage des informations par FID ###
            
            if fid_dic['fid'] != null_val:
                
                regex_in = r'^CURRENT CONTEXT --.*{0}$'.format(fid_dic['fid'])
                
                sfpshow_fid_lst = get_line_cmd(sfpshow_lst, regex_in, r'^sfpshow -all\s+:')
                switchshow_fid_lst = get_line_cmd(switchshow_lst, regex_in, r'^switchshow\s+:')
                porterrshow_fid_lst = get_line_cmd(porterrshow_lst, regex_in, r'^porterrshow\s+:')
                fabricshow_fid_lst = get_line_cmd(fabricshow_lst, regex_in, r'^fabricshow\s+:')
                cfgsize_fid_lst = get_line_cmd(cfgsize_lst, regex_in, r'^cfgsize\s+:')
                
            else:
                sfpshow_fid_lst = sfpshow_lst
                switchshow_fid_lst = switchshow_lst
                porterrshow_fid_lst = porterrshow_lst
                fabricshow_fid_lst = fabricshow_lst
                cfgsize_fid_lst = cfgsize_lst
            
            
            ### Récupération du Nom du Switch/WWN/Type/Zoneset/DB Size (Virtuel) ###
            
            get_switch_info(fid_dic, switchshow_fid_lst, cfgsize_fid_lst, null_val)
            
            ### Récupération du Master Switch (Fabric) ###
            
            get_pri_info(fid_dic, fabricshow_fid_lst, null_val)
            
            ### Generation Liste de sortie ###
            
            pri_csv_lst.append([
                sw_dic['name'],
                fid_dic['fid'],
                fid_dic['sw_name'],
                fid_dic['sw_master'],
                fid_dic['is_master']
            ])
            
                    
            sw_info_csv_lst.append([
                sw_dic['name'],
                fid_dic['fid'],
                fid_dic['sw_name'],
                'Brocade',
                sw_dic['sw_serial'],
                fid_dic['sw_type'],
                ' '.join(sw_dic['version_lst']),
                fid_dic['domain_id'],
                fid_dic['is_master'],
                fid_dic['sw_wwn'],
                fid_dic['sw_zone_set'],
                null_val, #'use_mem',
                null_val, #'tx_util_mem',
                null_val, #'tx_util_cpu',
                null_val, #'nb_conf',
                fid_dic['db_size'],
                sw_date,
            ])
            
            # print sw_info_csv_lst
            
            ### Vérification précense de Slot et Récuperation de la liste de port ###
            
            no_slot = False
            
            if [s.split()[1] for s in switchshow_fid_lst if re.search(r'\s*Index ', s)][0] == 'Slot':
                port_lst = ['{0}.{1}.{2}'.format(s.split()[1], s.split()[2], s.split()[0]) for s in switchshow_fid_lst if re.search(r'\s*[0-9]+\s+[0-9]+\s+[0-9]+', s)]
            
            else:
                port_lst = ['.{0}.{1}'.format(s.split()[1], s.split()[0]) for s in switchshow_fid_lst if re.search(r'\s*[0-9]+\s+[0-9]+\s+[0-9]+', s)]
                no_slot = True
                
            ### Lancement du Parsing par Port ###
                
            if port_lst:
                
                portshow_fid_lst = get_line_cmd(portshow_lst, r'^portshow {0}$'.format(port_lst[0].split('.')[-1]), r'^portcamshow {0}$'.format(port_lst[-1].split('.')[-1]))
                
                port_dic_lst = []
                
                for port in port_lst:
                    
                    port_dic = {}
                    
                    port_dic['wwn'] = null_val
                    port_dic['sfp'] = null_val
                    port_dic['type'] = null_val
                    port_dic['npiv_wwn_lst'] = [null_val]
                    
                    if no_slot:
                        port_dic['slot'] = null_val
                    else:
                        port_dic['slot'] = port.split('.')[0]
                    
                    
                    port_dic['port'] = port.split('.')[1]
                    port_dic['index'] = port.split('.')[2]
                    
                    ### Récupération des informations sur les Ports ###
                    
                    get_portname(port_dic, port_name_lst, null_val)
                    get_switchshow_info(port_dic, switchshow_fid_lst, no_slot, null_val)
                    get_sfpshow_info(port_dic, sfpshow_fid_lst, no_slot, null_val)
                    get_portshow_info(port_dic, portshow_fid_lst, port_dic['npiv'], null_val)
                    
                    ### Vérification des Champs OLS-LR in/out ###
                    
                    if port_dic['ols_in'] != port_dic['ols_out'] or port_dic['lr_in'] != port_dic['lr_out']:
                        olslr_csv_lst.append([
                            sw_dic['name'],    
                            sw_dic['sw_serial'], 
                            fid_dic['fid'],    
                            fid_dic['sw_name'],  
                            port_dic['slot'],    
                            port_dic['port'],    
                            port_dic['index'],   
                            port_dic['stats_clear'],   
                            port_dic['ols_in'],
                            port_dic['ols_out'],
                            port_dic['lr_in'],
                            port_dic['lr_out'],
                        ])
                    
                    ### Generation Liste de sortie par Port ###
                    
                    for wwn_virt in port_dic['npiv_wwn_lst']:
                        
                        if wwn_virt == null_val:
                            wwn_virt = port_dic['wwn']
                        
                        wwn_csv_lst.append([
                            sw_dic['sw_ms'],
                            sw_dic['sw_loc'],
                            sw_dic['name'],
                            fid_dic['fid'],
                            fid_dic['sw_name'],
                            fid_dic['sw_wwn'],
                            port_dic['index'],
                            port_dic['slot'],
                            port_dic['port'],
                            port_dic['type'],
                            port_dic['speed'],
                            port_dic['state'],
                            port_dic['wwn'],
                            wwn_virt,
                            port_dic['port_name'],
                            sw_date,
                        ])
                    
                    if port_dic['sfp'] != null_val:
                        sfp_csv_lst.append([
                            sw_dic['name'],    
                            sw_dic['sw_serial'],    
                            fid_dic['fid'],    
                            fid_dic['sw_name'],    
                            port_dic['slot'],    
                            port_dic['port'],    
                            port_dic['index'],    
                            port_dic['sfp_connector'],    
                            port_dic['sfp_serial_num'],    
                            port_dic['sfp_part_num'],    
                            port_dic['sfp_transceiver'],    
                            port_dic['sfp_av_speed'],    
                            port_dic['sfp_lenght_9u_km'],    
                        ])
                
        logger.info('SW:{0} Write Files on Output File'.format(sw_dic['name']))   
        
        ### Ecriture des données dans les CSV si non locké par un autre processus ###
        
        with lock:
            for csv_file_dic in csv_file_dic_lst:
                with open(csv_file_dic['file'], 'a', 0) as outfile:
                    csv_writer = csv.writer(outfile, delimiter=';')
                    map(csv_writer.writerow, eval(csv_file_dic['type'].lower() + '_csv_lst'))
        
    ## Fin de la collecte ##
    
    sw_execution_time = time.strftime("%H:%M:%S", time.gmtime((time.time() - sw_start_time)))
    logger.info('PID:{0}, P.NAME:{1}, SW:{2} [done][T:{3}]'.format(pid, name, sw_dic['name'], sw_execution_time))
    
def supshow_parser_manage(logger, input_path, lock, supshow_input_lst, csv_file_dic_lst, null_val, debug_mode=False):
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
    
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    
    LOG_FILE = LOG_PATH + '/supshow_parse_port.log'
    
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
    
    ### Vérification existance Input Path ###
    
    LOGGER.info('input Path : {0}'.format(INPUT_PATH))
    
    if not os.path.exists(INPUT_PATH):
        LOGGER.error('Input Path Not Find {0} ! exit()'.format(INPUT_PATH))
        sys.exit(1)
    
    ### Création[Suppression] du Répertoire 'OUTPUT_PATH' ###
    
    LOGGER.info('Output Path : {0}'.format(OUTPUT_PATH))
    
    if os.path.exists(OUTPUT_PATH):
        LOGGER.warning('Path : {0} Already Exist !. Script Delete it'.format(OUTPUT_PATH))
        
        try:
            shutil.rmtree(OUTPUT_PATH)
            LOGGER.info('Delete {0} [done]'.format(OUTPUT_PATH))
        except:
            LOGGER.error('Delete {0} Problem ! exit()'.format(OUTPUT_PATH))
            sys.exit(1)
        
    try:
        os.makedirs(OUTPUT_PATH)
        LOGGER.info('Create {0} [done]'.format(OUTPUT_PATH))
        
    except:
        LOGGER.error('Creation {0} Problem ! exit()'.format(OUTPUT_PATH))
        sys.exit(1)
    
    ### Déclaration des fichiers Output ###
    
    csv_file_dic_lst = [
        {
            'type': 'WWN',
            'file': '{0}/supshow_parse_{1}_{2}.csv'.format(OUTPUT_PATH, 'wwn', time.strftime("%Y_%m_%d")),
            'header': ['Meta_San', 'Site', 'Chassis', 'Fid', 'Switch', 'WWN_Switch', 'Index', 'Slot', 'Port', 'Type', 'Speed', 'Status', 'WWN_Phys', 'WWN_Virt', 'Alias', 'Date'],
        },
        
        {
            'type': 'SLOT',
            'file': '{0}/supshow_parse_{1}_{2}.csv'.format(OUTPUT_PATH, 'slot', time.strftime("%Y_%m_%d")),
            'header': ['chassis', 'chassis_sn', 'slot', 'blade_function', 'blade_type_id', 'model', 'status', 'serial_num', 'part_num'],
        },
        {
            'type': 'SFP',
            'file': '{0}/supshow_parse_{1}_{2}.csv'.format(OUTPUT_PATH, 'sfp', time.strftime("%Y_%m_%d")),
            'header': ['chassis', 'chassis_sn', 'fid', 'sw_name', 'slot', 'port', 'index', 'connector', 'serial_num', 'part_num', 'sfp_type', 'sfp_av_speed', 'sfp_lenght_9u_km'],
        },
        {
            'type': 'HRDW',
            'file': '{0}/supshow_parse_{1}_{2}.csv'.format(OUTPUT_PATH, 'hrdw', time.strftime("%Y_%m_%d")),
            'header': ['chassis', 'chassis_sn', 'hw_type', 'unit_id', 'serial_num', 'part_num'],
        },
        {
            'type': 'CP',
            'file': '{0}/supshow_parse_{1}_{2}.csv'.format(OUTPUT_PATH, 'cp', time.strftime("%Y_%m_%d")),
            'header': ['chassis', 'chassis_sn', 'sw_factory_sn', 'cp_name', 'version', 'status'],
        },
        {
            'type': 'PRI',
            'file': '{0}/supshow_parse_{1}_{2}.csv'.format(OUTPUT_PATH, 'pri', time.strftime("%Y_%m_%d")),
            'header': ['chassis', 'fid', 'sw_name', 'principal_name', 'principal'],
        },
        {
            'type': 'OLSLR',
            'file': '{0}/supshow_parse_{1}_{2}.csv'.format(OUTPUT_PATH, 'olslr', time.strftime("%Y_%m_%d")),
            'header': ['chassis', 'chassis_sn', 'fid', 'sw_name', 'slot', 'port', 'index', 'stats_clear', 'ols_in', 'ols_out', 'lr_in', 'lr_out'],
        },
        {
            'type': 'SW_INFO',
            'file': '{0}/supshow_parse_{1}_{2}.csv'.format(OUTPUT_PATH, 'sw_info', time.strftime("%Y_%m_%d")),
            'header': ['chassis', 'fid', 'sw_name', 'constructor', 'model', 'chassis_sn', 'version', 'domain_id', 'is_principal', 'switch_wwn', 'active_zone_set', 'use_mem', 'tx_util_mem', 'tx_util_cpu', 'nb_conf', 'db_size', 'date'],
        },
    ]
    
    ### Création des Headers et des fichiers CSV en Output (Write) ###
    
    for csv_file_dic in csv_file_dic_lst:
        header_maker(csv_file_dic['file'], csv_file_dic['header'])
    
    ### Récupération de la liste des fichiers du repertoire d'inuput (supportshow) ###
    
    supshow_file_all_lst = [f for f in os.listdir(INPUT_PATH) if re.search('^supportshow\..*\.out$', f)]
    
    if not supshow_file_all_lst:
        LOGGER.error('No supportshow File Find ! exit()')
        exit(1)
        
    ### Gestion des files de switch en fonction des threads ###
          
    sw_by_thread = int(len(supshow_file_all_lst)/thread_count)
            
    if sw_by_thread is 0:
        sw_by_thread += 1
    
    ### Generation d'une liste de liste de switch en fonction de la variable 'sw_by_thread' ###
    
    supshow_file_splt_lst = [supshow_file_all_lst[i:i + sw_by_thread] for i in xrange(0, len(supshow_file_all_lst), sw_by_thread)]
    
    LOGGER.info('Number of Switch  : {0}'.format(len(supshow_file_all_lst)))
    LOGGER.info('Number of Queue   : {0}'.format(len(supshow_file_splt_lst)))
    
    ### Lancement du parsing en Multithread ###
    
    LOGGER.info('> PARSING WWN START [{0}] <'.format(time.strftime("%H:%M:%S")))
    
    proc_lst = []
    
    for supshow_file_lst in supshow_file_splt_lst:
        
        lock = mp.Lock()
        proc = mp.Process(
            target = supshow_parser_manage,
            args = (
                LOGGER,
                INPUT_PATH,
                lock,
                sorted(supshow_file_lst),
                csv_file_dic_lst,
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
    
