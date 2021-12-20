config = {
    'studio': 'skill',
    'schema': 'ww',

    # table_mappings represents the mapping between the file name on the sftp server and the table name in vertica
    'table_mappings': {
        'bounce': 'responsys_bounce',
        'click': 'responsys_click',
        'fail': 'responsys_fail',
        'launch_state': 'responsys_launch_state',
        'open': 'responsys_open',
        'program': 'responsys_program',
        'sent': 'responsys_sent',
    }
}
