#!/usr/bin/perl

use 5.10.0;
use strict;

use Getopt::Long;
use Socket;

my @ips;

GetOptions(
    'ip|ips=s' => \@ips,
);

foreach my $ip (@ips) {
    print gethostbyaddr(inet_aton($ip), AF_INET) . "\n";
}

exit;
