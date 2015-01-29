#!/usr/bin/perl



use 5.10.1;
use strict;

use Data::Dumper;
use File::Slurp;
use Getopt::Long;

use bepress::IP2Location;
use Log;

my ($help, $force);
my ($file, $output_file);

GetOptions(
    'file=s'          => \$file,
    'o|out|outfile=s' => \$output_file,
    'h|help'          => \$help,
    'force'           => \$force,
);

die usage() if $help;

if (! -f $file) {
    die "unable to locate file $file!";
}

if (! $output_file) {
    $output_file = $file;
    $output_file =~ s/\.csv$/\_with_domains\.csv/;
}

if (-e $output_file && ! $force) {
    die "specified output file $output_file already exists!  To overwite, specify the --force option";
}

Log->info("will write to output file: $output_file");


eval {

    Log->debug("reading file $file");
    my @lines = File::Slurp::read_file($file);

    Log->debug("looking up IP domains");
    my $output_data = add_domains(\@lines);

    Log->debug("ordering entries by total counts");
    my $output_lines = order_by_counts($output_data);

    Log->debug("writing output file $output_file");
    File::Slurp::write_file($output_file, $output_lines);
    exit;
};
if ($@) {
    die $@;
}

exit;


sub order_by_counts {
    my ($output_data) = @_;

    my @ordered_ips = sort { $output_data->{$a}->{total_count} <=> $output_data->{$b}->{total_count} } keys %$output_data;
    my @output_lines;

    foreach my $ip (@ordered_ips) {
        if (exists($output_data->{$ip}->{t})) {
            push(@output_lines, join(',', @{$output_data->{$ip}->{t}}));
        }
        if (exists($output_data->{$ip}->{f})) {
            push(@output_lines, join(',', @{$output_data->{$ip}->{f}}));
        }
    }

    return \@output_lines;
}


sub add_domains {
    my ($lines) = @_; # a ref to an ARRAY of csv file lines to process

    my %output_data;
    my $ip2location = bepress::IP2Location->new();
    my $sqldb = $ip2location->retrieve_sqldb();

    foreach my $line (@$lines) {
        my ($ip_address, $counted_as_hit, $count) = split(',', $line);
        my $ip_data = $ip2location->lookup($ip_address);
        my $domain = $ip_data->{domain};
    
        if (!defined($output_data{$ip_address})) {
            $output_data{$ip_address}->{total_count} = 0;
        }
    
        $output_data{$ip_address}->{total_count} += $count;
        $output_data{$ip_address}->{$counted_as_hit} = [$ip_address, $domain, $counted_as_hit, $count],
    }

    return \%output_data;
}


sub usage {
    print "\nUsage:\n\n".
          "user\@host> $0 --file=<csv file> --o|out|outfile=<output csv file> [--h|help] [--force]\n\n";
    exit;
}
