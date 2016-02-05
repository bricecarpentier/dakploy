"""
Database backups
"""

from fabric.api import (
    cd,
    env,
    puts,
    run,
    settings,
    sudo,
    task,
)
from fabric.colors import green

from fabtools import require
from fabtools.files import is_dir, is_file
import fabtools


DEFAULT_BACKUP_SCRIPT = '''#!/bin/bash

###########################
####### LOAD CONFIG #######
###########################

source /etc/default/{config_name}


###########################
#### PRE-BACKUP CHECKS ####
###########################

# Make sure we're running as the required backup user
if [ ! -z "$BACKUP_USER" -a "$(id -un)" != "$BACKUP_USER" ]; then
    echo "This script must be run as $BACKUP_USER. Exiting."
    exit 1;
fi;


###########################
### INITIALISE DEFAULTS ###
###########################

if [ ! $HOSTNAME ]; then
    HOSTNAME="localhost"
fi;

if [ ! $USERNAME ]; then
    USERNAME="postgres"
fi;


###########################
#### START THE BACKUPS ####
###########################

function perform_backups()
{{
    SUFFIX=$1
    FINAL_BACKUP_DIR=$BACKUP_DIR"`date +\%Y-\%m-\%d`$SUFFIX/"

    echo "Making backup directory in $FINAL_BACKUP_DIR"

    if ! mkdir -p $FINAL_BACKUP_DIR; then
        echo "Cannot create backup directory in $FINAL_BACKUP_DIR. Go and fix it!"
        exit 1;
    fi;


    ###########################
    ### SCHEMA-ONLY BACKUPS ###
    ###########################

    for SCHEMA_ONLY_DB in ${{SCHEMA_ONLY_LIST//,/ }}
    do
            SCHEMA_ONLY_CLAUSE="$SCHEMA_ONLY_CLAUSE or datname ~ '$SCHEMA_ONLY_DB'"
    done

    SCHEMA_ONLY_QUERY="select datname from pg_database where false $SCHEMA_ONLY_CLAUSE order by datname;"

    echo -e "\n\nPerforming schema-only backups"
    echo -e "--------------------------------------------\n"

    SCHEMA_ONLY_DB_LIST=`psql -h "$HOSTNAME" -U "$USERNAME" -At -c "$SCHEMA_ONLY_QUERY" postgres`

    echo -e "The following databases were matched for schema-only backup:\n${{SCHEMA_ONLY_DB_LIST}}\n"

    for DATABASE in $SCHEMA_ONLY_DB_LIST
    do
            echo "Schema-only backup of $DATABASE"

            if ! pg_dump -Fp -s -h "$HOSTNAME" -U "$USERNAME" "$DATABASE" | gzip > $FINAL_BACKUP_DIR"$DATABASE"_SCHEMA.sql.gz.in_progress; then
                    echo "[!!ERROR!!] Failed to backup database schema of $DATABASE"
            else
                    mv $FINAL_BACKUP_DIR"$DATABASE"_SCHEMA.sql.gz.in_progress $FINAL_BACKUP_DIR"$DATABASE"_SCHEMA.sql.gz
            fi
    done


    ###########################
    ###### FULL BACKUPS #######
    ###########################

    for SCHEMA_ONLY_DB in ${{SCHEMA_ONLY_LIST//,/ }}
    do
        EXCLUDE_SCHEMA_ONLY_CLAUSE="$EXCLUDE_SCHEMA_ONLY_CLAUSE and datname !~ '$SCHEMA_ONLY_DB'"
    done

    FULL_BACKUP_QUERY="select datname from pg_database where not datistemplate and datallowconn $EXCLUDE_SCHEMA_ONLY_CLAUSE order by datname;"

    echo -e "\n\nPerforming full backups"
    echo -e "--------------------------------------------\n"

    for DATABASE in `psql -h "$HOSTNAME" -U "$USERNAME" -At -c "$FULL_BACKUP_QUERY" postgres`
    do
        if [ $ENABLE_PLAIN_BACKUPS = "yes" ]
        then
            echo "Plain backup of $DATABASE"

            if ! pg_dump -Fp -h "$HOSTNAME" -U "$USERNAME" "$DATABASE" | gzip > $FINAL_BACKUP_DIR"$DATABASE".sql.gz.in_progress; then
                echo "[!!ERROR!!] Failed to produce plain backup database $DATABASE"
            else
                mv $FINAL_BACKUP_DIR"$DATABASE".sql.gz.in_progress $FINAL_BACKUP_DIR"$DATABASE".sql.gz
            fi
        fi

        if [ $ENABLE_CUSTOM_BACKUPS = "yes" ]
        then
            echo "Custom backup of $DATABASE"

            if ! pg_dump -Fc -h "$HOSTNAME" -U "$USERNAME" "$DATABASE" -f $FINAL_BACKUP_DIR"$DATABASE".custom.in_progress; then
                echo "[!!ERROR!!] Failed to produce custom backup database $DATABASE"
            else
                mv $FINAL_BACKUP_DIR"$DATABASE".custom.in_progress $FINAL_BACKUP_DIR"$DATABASE".custom
            fi
        fi

    done

    echo -e "\nAll database backups complete!"

    # Upload to S3
    if [ ! -z "$S3_PREFIX" ]
    then
        /usr/local/bin/s3cmd put --recursive $FINAL_BACKUP_DIR s3://$S3_PREFIX/$(basename $FINAL_BACKUP_DIR)/
    fi

}}

# MONTHLY BACKUPS

DAY_OF_MONTH=`date +%d`

if [ $DAY_OF_MONTH = "1" ];
then
    # Delete all expired monthly directories
    find $BACKUP_DIR -maxdepth 1 -name "*-monthly" -exec rm -rf '{{}}' ';'

    perform_backups "-monthly"

    # exit 0;
fi

# WEEKLY BACKUPS

DAY_OF_WEEK=`date +%u` #1-7 (Monday-Sunday)
EXPIRED_DAYS=`expr $((($WEEKS_TO_KEEP * 7) + 1))`

if [ $DAY_OF_WEEK = $DAY_OF_WEEK_TO_KEEP ];
then
    # Delete all expired weekly directories
    find $BACKUP_DIR -maxdepth 1 -mtime +$EXPIRED_DAYS -name "*-weekly" -exec rm -rf '{{}}' ';'

    perform_backups "-weekly"

    # exit 0;
fi

# DAILY BACKUPS

# Delete daily backups 7 days old or more
find $BACKUP_DIR -maxdepth 1 -mtime +$DAYS_TO_KEEP -name "*-daily" -exec rm -rf '{{}}' ';'


perform_backups "-daily"

FINAL_BACKUP_DIR=$BACKUP_DIR"`date +\%Y-\%m-\%d`$SUFFIX/"

tree -h $FINAL_BACKUP_DIR

'''

DEFAULT_BACKUP_CONFIG = '''##############################
## POSTGRESQL BACKUP CONFIG ##
##############################

# Optional system user to run backups as.  If the user the script is running as doesn't match this
# the script terminates.  Leave blank to skip check.
BACKUP_USER=postgres

# Optional hostname to adhere to pg_hba policies.  Will default to "localhost" if none specified.
HOSTNAME=/var/run/postgresql/

# Optional username to connect to database as.  Will default to "postgres" if none specified.
USERNAME=postgres

# This dir will be created if it doesn't exist.  This must be writable by the user the script is
# running as.
BACKUP_DIR=/var/backups/postgresql/

# List of strings to match against in database name, separated by space or comma, for which we only
# wish to keep a backup of the schema, not the data. Any database names which contain any of these
# values will be considered candidates. (e.g. "system_log" will match "dev_system_log_2010-01")
SCHEMA_ONLY_LIST=""

# Will produce a custom-format backup if set to "yes"
ENABLE_CUSTOM_BACKUPS=yes

# Will produce a gzipped plain-format backup if set to "yes"
ENABLE_PLAIN_BACKUPS=yes


#### SETTINGS FOR ROTATED BACKUPS ####

# Which day to take the weekly backup from (1-7 = Monday-Sunday)
DAY_OF_WEEK_TO_KEEP=5

# Number of days to keep daily backups
DAYS_TO_KEEP=7

# How many weeks to keep weekly backups
WEEKS_TO_KEEP=5


#### S3 PREFIX (BUCKET + OPTIONAL DIR) TO UPLOAD TO ####

S3_PREFIX={aws_s3_prefix}

######################################
'''

DEFAULT_S3_CFG = '''[default]
access_key = {aws_access_key_id}
add_encoding_exts = 
add_headers = 
bucket_location = EU
cache_file = 
cloudfront_host = cloudfront.amazonaws.com
default_mime_type = binary/octet-stream
delay_updates = False
delete_after = False
delete_after_fetch = False
delete_removed = False
dry_run = False
enable_multipart = True
encoding = UTF-8
encrypt = False
follow_symlinks = False
force = False
get_continue = False
gpg_command = /usr/bin/gpg
gpg_decrypt = %(gpg_command)s -d --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_encrypt = %(gpg_command)s -c --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s %(input_file)s
gpg_passphrase = 
guess_mime_type = True
host_base = s3.amazonaws.com
host_bucket = %(bucket)s.s3.amazonaws.com
human_readable_sizes = False
invalidate_default_index_on_cf = False
invalidate_default_index_root_on_cf = True
invalidate_on_cf = False
list_md5 = False
log_target_prefix = 
mime_type = 
multipart_chunk_size_mb = 15
preserve_attrs = True
progress_meter = True
proxy_host = 
proxy_port = 0
recursive = False
recv_chunk = 4096
reduced_redundancy = False
secret_key = {aws_secret_access_key}
send_chunk = 4096
simpledb_host = sdb.amazonaws.com
skip_existing = False
socket_timeout = 300
urlencoding_mode = normal
use_https = True
verbosity = WARNING
website_endpoint = http://%(bucket)s.s3-website-%(location)s.amazonaws.com/
website_error = 
website_index = index.html
'''


def setup_postgresql_backups(instance_name, aws_s3_prefix, aws_access_key_id, aws_secret_access_key, backup_script=None, backup_config=None):
    """
    Setup daily SQL dumps of the Postgres server to Amazon S3
    """

    backup_script = backup_script if backup_script is not None else DEFAULT_BACKUP_SCRIPT
    backup_config = backup_config if backup_config is not None else DEFAULT_BACKUP_CONFIG

    puts(green('Setting up Postgresql backups'))

    # Cleanup old config files
    sudo('rm -f /etc/default/pg_backup')
    sudo('rm -f /etc/cron.d/pg_backup')

    # Skip if backups are not enabled
    if not env.enable_backups:
        puts('Backups not enabled, skipping...')
        return

    # Skip if backups are not configured
    if not aws_s3_prefix:
        puts('Target bucket not configured, skipping...')
        return

    setup_s3cmd(aws_access_key_id, aws_secret_access_key)

    require.deb.package('tree')

    # Postgres backup script
    backup_script_path = '/usr/local/bin/pg_backup_rotated.sh'
    config_name = 'pg_backup_{}'.format(instance_name)

    require.file(
        backup_script_path,
        contents=backup_script.format(config_name=config_name),
        owner='root',
        group='root',
        mode='0755',
        use_sudo=True
    )

    # Postgres backup configuration file
    require.file(
        '/etc/default/{}'.format(config_name),
        contents=backup_config.format(aws_s3_prefix=aws_s3_prefix),
        owner='root',
        group='root',
        mode='0644',
        use_sudo=True
    )

    # Backups directory
    require.directory('/var/backups/postgresql', owner='postgres', use_sudo=True)

    # Add crontab entry
    fabtools.cron.add_task(config_name, '50 1 * * *', 'postgres', backup_script_path)


def setup_s3cmd(aws_access_key_id, aws_secret_access_key, s3cfg=None):
    """
    Install s3cmd for multipart uploads to Amazon S3
    """

    s3cfg = s3cfg if s3cfg is not None else DEFAULT_S3_CFG

    # Install s3cmd
    if not is_file('/usr/local/bin/s3cmd'):
        require.file(url='http://sourceforge.net/projects/s3tools/files/s3cmd/1.5.0-alpha1/s3cmd-1.5.0-alpha1.tar.gz')
        if not is_dir('s3cmd-1.5.0-alpha1'):
            run('tar xzf s3cmd-1.5.0-alpha1.tar.gz')
        with cd('s3cmd-1.5.0-alpha1'):
            require.deb.package('python-setuptools')
            sudo('python setup.py install')

    # Optional dependencies
    require.python.package('python-magic', use_sudo=True)

    # S3 config file (including credentials)
    require.file(
        '/var/lib/postgresql/.s3cfg',
        contents=s3cfg.format(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key),
        use_sudo=True
    )


@task
def backup():
    """
    Backup PostgreSQL database to Amazon S3 now
    """
    with settings(sudo_prefix="sudo -H -S -p '%(sudo_prompt)s' "):  # add the -H flag
        sudo('/usr/local/bin/pg_backup_rotated.sh', user='postgres')


# Prevent tasks from appearing twice in task list
__all__ = []
