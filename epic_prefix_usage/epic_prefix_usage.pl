#!/usr/bin/perl
# 
# this script generates a csv file per day with prefixes and the number of PID's in it.
#
# It executes the following commands:
#
# find the prefixes:
# select * from nas;
# find the PID's per prefix:
# select count(*) from handles where type='HS_ADMIN' and handle like '11100%';
#
# It should be executed at 23:00 at night...
#

use strict;
use warnings;

use Data::Dumper;
use DBI;
use Getopt::Long;
use Switch;
use Text::CSV;
use Time::Local;
use Pod::Usage;

my %settings = (  
                  'csv' => {
                        'dir'       => '/tmp',
                        'file'      => 'new.csv',
                  },
                  'dbase' => {
                         'dbase'  => '<database>',
                         'driver' => 'mysql',
                         'host'   => 'localhost',
                         'user'   => '<username>',
                         'passwd' => '<password>',
                  },
                  'debug' => 'False',
);


#
# main program
#

# process arguments
read_args(\%settings);

# read credentials file
read_credentials(\%settings);

# contruct an epoch today at 23:00 hours
construct_epoch_today_2300(\%settings); 

# connect database
my $dbh = connect_database(\%settings);
print ">>$dbh<<\n" if $settings{debug} =~ /True/ ;

# query for all prefixes in the database
my $sth = query_nas($dbh,\%settings);

# store all prefixes in a hash
store_nas($sth,\%settings);

# query for each prefix the number of handles/PID's in the database
foreach my $prefix (sort keys %{ $settings{nas} } ) {
   print "prefix: $prefix \n" if $settings{debug} =~ 'True';
   $sth = query_prefix_count($prefix,$dbh,\%settings);
   store_prefix_count($prefix,$sth,\%settings);
}

# disconnect the database
disconnect_database($dbh);

# print the results
print Dumper $settings{nas} if $settings{debug} =~ 'True';

create_csv_file(\%settings);

exit;

#
# subroutines
#

sub read_args {

    my $settings_ref = shift;
    my $help='';
    my $fullhelp='';
    my $debug='';
    my $postgres='';

    #
    # process the options/arguments
    #
    GetOptions ('dir=s'         => \$settings_ref->{csv}->{dir},
                'file=s'        => \$settings_ref->{csv}->{file},
                'credentials=s' => \$settings_ref->{credentials},
                'driver=s'      => \$settings_ref->{dbase}->{driver},
                'help'          => \$help,
	        'fullhelp'      => \$fullhelp,
                'debug'         => \$debug
               );

    if ( $debug ) {
        $settings_ref->{debug} = 'True';
    }
    if( $help ) {
        pod2usage(2);
    }
    if( $fullhelp ) {
        pod2usage(1);
    }


    return;     
}

sub read_credentials {

    my $settings_ref = shift;
    my $sql_url = '';
    my $sql_driver ='';
    my $sql_login = '';
    my $sql_passwd = '';

    #
    # read a credentials file and use the output to set the credentials correctly.
    # it is tailored for config.dct of the handle system
    #
    # "sql_url" = "jdbc:mysql://epic2.sara.nl/7_master"
    # "sql_url" = "jdbc:mysql:///eudat2" 
    # "sql_login" = "eudat" 
    # "sql_passwd" = "<password>"

    open( my $input_fh, "<", $settings_ref->{credentials} ) || die "Can't open $settings_ref->{credentials}: $!";

    while ( defined( my $line = <$input_fh> ) ) {
       chomp $line;
       $line =~ s/\s//g;
       $line =~ s/\"//g;
       print "$line \n" if $settings_ref->{debug} =~ /True/ ;
       if ( $line =~ /sql_[d|l|p|u]/ ) {
           print "$line \n" if $settings_ref->{debug} =~ /True/ ;
           my @key_values = split("=", $line);
           print "Key:=$key_values[0]=, value:=$key_values[1]=\n" if $settings_ref->{debug} =~ /True/ ;
           switch ($key_values[0]) {
               case 'sql_login' {
                   $settings_ref->{dbase}->{user} = $key_values[1];
               }
               case 'sql_passwd' {
                   $settings_ref->{dbase}->{passwd} = $key_values[1];
               }
               case 'sql_url' {
                   my @jdbc = split("/",$key_values[1]);
                   $settings_ref->{dbase}->{dbase} = $jdbc[-1];
                   if ( defined $jdbc[-2] && $jdbc[-2] ne "" ) {
                       $settings_ref->{dbase}->{host}  = $jdbc[-2];
                   }
               }
           }
       }
    }

    close $input_fh or warn "Unable to close the file handle: $!";;

    return;
}


sub construct_epoch_today_2300 {

    my $settings_ref = shift;

    #
    # construct the epoch of today 23:00 hours
    #
    ($settings_ref->{date}->{sec},
     $settings_ref->{date}->{min},
     $settings_ref->{date}->{hour},
     $settings_ref->{date}->{mday},
     $settings_ref->{date}->{mon},
     $settings_ref->{date}->{year},
     $settings_ref->{date}->{wday},
     $settings_ref->{date}->{yday},
     $settings_ref->{date}->{isdst}) = localtime(time);
    $settings_ref->{date}->{year} += 1900;
    $settings_ref->{date}->{today_2300} = timelocal(0,0,23,$settings_ref->{date}->{mday},$settings_ref->{date}->{mon},$settings_ref->{date}->{year});
    print "epoch time = $settings_ref->{date}->{today_2300}\n" if $settings_ref->{debug} =~ /True/ ; 

    return;
}

sub connect_database {

    my $settings_ref = shift;

    #
    # Connect to database
    #
    print "DB params: $settings_ref->{dbase}->{driver}:database=$settings_ref->{dbase}->{dbase};host=$settings_ref->{dbase}->{host}\n" if $settings_ref->{debug} =~ /True/ ;
    my $dbh = DBI->connect(
        "DBI:$settings_ref->{dbase}->{driver}:database=$settings_ref->{dbase}->{dbase};host=$settings_ref->{dbase}->{host}",
        $settings_ref->{dbase}->{user},
        $settings_ref->{dbase}->{passwd},
        {
            RaiseError => 1,
            PrintError => 1,
        }
    ) or die $DBI::errstr;

    return $dbh;
}

sub query_nas {

    my $dbh = shift;
    my $settings_ref = shift;

    #
    # query all NAS entries from the database
    #
    my $sth = $dbh->prepare(" SELECT * from nas") or die $dbh->errstr;

    $sth->execute() or die $dbh->errstr;

    return $sth;
}

sub store_nas {

    my $sth = shift;
    my $settings_ref = shift;

    #
    # create a hash with the results and count of 0
    #
    print "nas\n" if $settings_ref->{debug} =~ 'True';
    while (my $results = $sth->fetchrow_hashref) {
      my $na = $results->{na};
      $na =~ s/.+\///;
      print "$results->{na} --> $na \n" if $settings_ref->{debug} =~ 'True';

      $settings_ref->{nas}->{$na} = 0;
   }

   return;
}

sub query_prefix_count {

    my $prefix = shift;
    my $dbh = shift;
    my $settings_ref = shift;

    #
    # query the count of prefix entries from the database
    #
    print "Perform \"SELECT count(*) from handles where type='HS_ADMIN' and handle like '${prefix}%'\" \n" if $settings_ref->{debug} =~ 'True';
    my $sth = $dbh->prepare("SELECT count(*) from handles where type='HS_ADMIN' and handle like '${prefix}%'") or die $dbh->errstr;

    $sth->execute() or die $dbh->errstr;

    return $sth;
}

sub store_prefix_count {

    my $prefix = shift;
    my $sth = shift;
    my $settings_ref = shift;

    #
    # put the results in a hash
    #
    while (my $results = $sth->fetchrow_hashref) {
       
       $settings_ref->{nas}->{$prefix} = $results->{'count(*)'};
       print "prefix: $prefix, count: $settings_ref->{nas}->{$prefix}\n" if $settings_ref->{debug} =~ 'True';

    }

   return;
}

sub disconnect_database {

   my $dbh = shift;

   #
   # disconnect database
   #
   $dbh->disconnect;

   return;
}

sub create_csv_file {

    my $settings_ref = shift;
    my @csv_array;
    my $csv = Text::CSV->new ({ binary => 1, eol => $/});
    my $csv_file = "$settings_ref->{csv}->{dir}/$settings_ref->{date}->{today_2300}_$settings_ref->{csv}->{file}";

    #
    # query for each prefix the number of handles/PID's in the database
    # and put them in a csv file
    #
    foreach my $prefix (sort keys %{ $settings_ref->{nas} } ) {
       print "prefix: $prefix count: $settings_ref->{nas}->{$prefix} \n" if $settings_ref->{debug} =~ 'True';
       push @csv_array, [$settings_ref->{date}->{today_2300}, $prefix, $settings_ref->{nas}->{$prefix}];
    }

    $csv->eol ("\r\n");
    open my $fh, ">:encoding(utf8)", $csv_file or die "$csv_file: $!";
    $csv->print ($fh, $_) for @csv_array;
    close $fh or die "$csv_file: $!";

    return;
}

__END__

=head1 NAME

epic_prefix_usage.pl

=head1 SYNOPSIS

epic_prefix_usage.pl B<--credentials> I<credentials>  B<--dir> I<dir>  B<--file> I<file>

epic_prefix_usage.pl B<--credentials> I<credentials>  B<--dir> I<dir>  B<--file> I<file> B<--debug>

epic_prefix_usage.pl B<--credentials> I<credentials>  B<--dir> I<dir>  B<--file> I<file> B<--driver> I<driver>

epic_prefix_usage.pl B<--help>


----------------

Please use --fullhelp for explanation of all options

=head1 DESCRIPTION

This program cabn be used to retrieve the number of PID's for each prefix in a handle server.
The output will be written to a csv file.

The procedure works as follows:
    find the prefixes:
    select * from nas;
       find the PID's per prefix:
       select count(*) from handles where type='HS_ADMIN' and handle like '<prefix>%';
    The status is given back in a csv file

It should be executed at 23:00 at night...

=head1 OPTIONS

=over 4

=item B<--credentials> I<credentials>

directory/filename of file with credentials. Standard config.dct

=item B<--dir> I<directory>

directory where to put the csv file

=item B<--driver> I<driver>

the driver to use to connect to the database (default is mysql) for postgres it is "Pg"

=item B<--file> I<file>

filename of the csv file. Will be added to "<epoch_time>_"

=item B<--debug>

Debug mode. Be verbose in what is being done and what the results of subroutines is.

=item B<--help>

Show this help text

=back

=head1 AUTHOR

Robert Verkerk <robert.verkerk@surfsara.nl>

Copyright (c) 2014-2016 by SURFsara bv


=cut
