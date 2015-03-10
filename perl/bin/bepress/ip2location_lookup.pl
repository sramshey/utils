#!/usr/bin/perl

use 5.10.1;
use strict;

use File::Slurp;
use Getopt::Long;

use Globals;
use Log;

use bepress::IP2Location qw( %IP2LOCATION_DB_FIELDS );

my (@ips, @fields, $db, $file);
my $help;

GetOptions(
    'ip|ips=s' => \@ips,
    'db=s'     => \$db,
    'f|file=s' => \$file,
    'field=s'  => \@fields,
    'h|help|?' => \$help,
);

usage() if $help;

if ($db) {
    my $db_supported = 0;
    foreach my $key (keys %IP2LOCATION_DB_FIELDS) {
        if ($db eq $key) {
            $db_supported = 1;
            last;
        }
    }
    if (!$db_supported) {
        Log->error("specified db product code $db is not currently supported!");
        exit 1;
    }
}

if (!@ips && !$file) {
    Log->error("you must specify at least one IP address or input file!");
    usage();
}

eval {

    if ($file) {
        if (! -f $file) {
            Log->error("unable to locate specified input file $file!");
            exit 1;
        }
        Log->debug("reading IP address from input file $file");
        @ips = read_file($file, chomp => 1);
    }

    if (!$db) {
        my $ip2location_authentication = Globals->get('ip2location-authentication');
        $db = $ip2location_authentication->{product_code};
        if (!$db) {
            Log->error("no IP2Location product code found in Globals - unable to determine correct product code to use!");
            exit 1;
        }
    }

    if (@fields) {
        my %avail_fields = map { $_ => 1 } @{$IP2LOCATION_DB_FIELDS{$db}};
        foreach my $field (@fields) {
            if (!$avail_fields{$field}) {
                Log->error("field '$field' is not available in $db!");
                usage();
            }
        }
        Log->debug("restricting output to user-specified fields: ".join(',', @fields)."\n");
    }

    Log->debug("Looking up ".scalar(@ips)." ip address".scalar(@ips) > 1 ? "es" : "");

    my $ip2location = bepress::IP2Location->new();

    if (!@fields) {
        @fields = @{$IP2LOCATION_DB_FIELDS{$db}};
    }

    foreach my $ip (@ips) {
        chomp($ip);
        my $ip_data = $ip2location->lookup($ip, \@fields);

        my @vals;
        foreach my $field (@fields) {
            push(@vals, $ip_data->{$field});
        }
        print join(",", ($ip, @vals))."\n";
    }
};
if ($@) {
    die $@;
}


exit;


sub usage {
    my $avail_fields = "";
    foreach my $db (sort keys %IP2LOCATION_DB_FIELDS) {
        $avail_fields .= "\t$db: ".join(',', @{$IP2LOCATION_DB_FIELDS{$db}})."\n";
    }
    print "\nUsage:\n\n".
          "    user\@host> $0 --ip=<ip address> [--ip=<ip address>] [--field=<field>] [--db=<version>]\n\nor\n".
          "    user\@host> $0 --file=<file> [--field=<field>] [--db=<version>]\n\n".
          "Input file format is one IP address per line.\n".
          "Supported database product codes are: ".join(',', sort keys %IP2LOCATION_DB_FIELDS)."\n".
          "Available fields are:\n".
          "$avail_fields\n";
    exit;
}
