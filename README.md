# Supshow-Parsing

Les scripts suivants permettent de collecter les supportshows puis de les parser pour récupérer les informations et les écrirent dans des fichiers csv. Ils fonctionnent dans cet ordre :

## fos_cmd_collect.py : Récupère les Supportshow ou tout autres commandes sur les switchs Brocades et de les enregistrent en local sur le serveur hôtes par switch et commande.

## supshow_parse_port.py : Parse les supportshows en entrée et récupère les informations liés au port (type de port, wwn, sfp) et d’autre information sur le switch (Hardware, slot, switch primaire, …)

## supshow_parse_zone.py : Parse les supportshows en entrée et récupère les informations liés au alias/zone. Le script se base sur le fichier en sortie du script supshow_parse_port.py qui récupère les switchs primaires afin de récupérer les informations uniquement sur ces derniers.

## supshow_fmt_clean.py : Récupère en entrée les fichiers et les fichiers de supportshow générés et les formates afin de liés les WWN récupéré avec supshow_parse_port.py avec les zone/alias récupérés avec supshow_parse_zone.py. Permet aussi d’archiver tous les fichiers (Compression BZ2) et de pousser les csv vers le répertoire final (Remplacement des anciens)

## supshow_exec.sh : Ce dernier est un script chapeau permettant d’exécuter tous les scripts dans l’ordre donné et de gérer les codes retours de chacun.

Tous ces modules sont codés en Python 2 excepté le script chapeau qui est en Shell.
