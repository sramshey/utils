#!/usr/bin/perl

use 5.10.1;
use strict;

use File::Slurp;
use Getopt::Long;

use bepress::IP2Location;

my @IP2Location_Fields = (
    'longitude',
    'country_name',
    'region_name',
    'country_code',
    'zip_code',
    'ip_from',
    'domain',
    'ip_to',
    'latitude',
    'city_name',
    'isp'
);

my (@ips, @fields, $file);
my $help;

GetOptions(
    'ip|ips=s' => \@ips,
    'f|file=s' => \$file,
    'field=s'  => \@fields,
    'h|help|?' => \$help,
);

usage() if $help;

if (!@ips && !$file) {
    print "you must specify at least one IP address or input file!";
    usage();
}

eval {

    if ($file) {
        if (! -f $file) {
            die "unable to locate specified input file $file!";
        }
        Log->debug("reading IP address from input file $file");
        @ips = read_file($file, chomp => 1);
    }

    if (@fields) {
        my %avail_fields = map { $_ => 1 } @IP2Location_Fields;
        foreach my $field (@fields) {
            if (!$avail_fields{$field}) {
                Log->error("unknown field '$field'!");
                usage();
            }
        }
        Log->debug("restricting output to user-specified fields: ".join(',', @fields)."\n");
    }

    Log->debug("Looking up ".scalar(@ips)." ip address".scalar(@ips) > 1 ? "es" : "");

    my $ip2location = bepress::IP2Location->new();
    my $sqldb = $ip2location->retrieve_sqldb();

    foreach my $ip (@ips) {
        chomp($ip);
        my $ip_data = $ip2location->lookup($ip);

        if (!@fields) {
            @fields = @IP2Location_Fields;
        }

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
    print "\nUsage:\n\n".
          "    user\@host> $0 --ip=<ip address> [--ip=<ip address>] [--field=<field>]\n\nor\n".
          "    user\@host> $0 --file=<file> [--field]\n\n".
          "Input file format is one IP address per line.\n".
          "Available fields are: ".join(',', @IP2Location_Fields)."\n\n";
    exit;
}
