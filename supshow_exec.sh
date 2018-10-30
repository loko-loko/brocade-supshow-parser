#!/bin/sh

PYTHON_BIN='/usr/bin/python'
SCRIPT_PATH='/sansto/bin/fos/supshow_script'

py_script_exec() {
    py_script=$1

    echo "/////////////////////// ${py_script} [$(date '+%Y/%m/%d %H:%M:%S')] [Start] ///////////////////////"
    echo

    $PYTHON_BIN ${py_script} 2>&-
    return_cmd=$?

    echo
    echo -e "/////////////////////// ${py_script} [$(date '+%Y/%m/%d %H:%M:%S')] \c"

    [[ $return_cmd != 0 ]] && { echo '[!Fail] ///////////////////////'; exit 1; } || echo '[Done] ///////////////////////'

}

ALL_R=0; COLLECT=0; PARSE_PORT=0; PARSE_ZONE=0

for i in $(seq $#); do
    
    eval "ARG"="\$$i"
    
    case $ARG in
        '-all'  ) ALL_R=1;;
        '-col'  ) COLLECT=1;;
        '-port' ) PARSE_PORT=1;;
        '-zone' ) PARSE_ZONE=1;;
        *       ) echo "<!> Bad Argument(s) [-all|-col|-port|-zone]"; exit 1;;
    esac
done

[[ $# == 0 ]] && ALL_R=1
[[ $ALL_R == 1 ]] && { COLLECT=1; PARSE_PORT=1; PARSE_ZONE=1; }

echo '================================ Collect Start ================================'

[[ $COLLECT == 1 ]] && py_script_exec "${SCRIPT_PATH}/fos_cmd_collect.py --command supportshow"
[[ $PARSE_PORT == 1 ]] && py_script_exec "${SCRIPT_PATH}/supshow_parse_port.py"
[[ $PARSE_ZONE == 1 ]] && py_script_exec "${SCRIPT_PATH}/supshow_parse_zone.py"

[[ $PARSE_PORT == 1 && $PARSE_ZONE == 1 ]] && py_script_exec "${SCRIPT_PATH}/supshow_fmt_clean.py"

echo '================================ Collect End ================================'

