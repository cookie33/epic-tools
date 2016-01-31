ePIC prefix usage
=================

This directory contains an ePIC tool, which is used to generate prefix usage statistics for ePIC members.

You can read more about ePIC under: [www.pidconsortium.eu](http://www.pidconsortium.eu/).

=======
# Usage statistics

There is 1 process at work at the moment:
  * usage statistics

## usage statistics on the epic node

The process for the usage statistics works as follows on the epic nodes:

1. `epic_prefix_usage.pl` retrieves the handles per prefix and put's it in a file per day.
   install the file in a directory
```
   cp <src_dir>/epic_prefix_usage.pl <dest_dir>
```

   In the crontab it states:
```
   # retrieve the number of handles per prefix in a database and put it in a file
0       23      *       *       *       <handle_user> <dest_dir>/epic_prefix_usage.pl --cred <dir_for_config_dct>/config.dct --dir <output_dir> --file <filename_to_append>.csv
```
