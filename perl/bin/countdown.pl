#!/usr/bin/perl

use strict;
use 5.10.0;

use Getopt::Long;
use DateTime;

my ($help, $verbose, $years, $months, $days, $hours, $minutes, $seconds, $message);

GetOptions(
	'years=i'     => \$years,
	'months=i'    => \$months,
	'days=i'      => \$days,
	'hours=i'     => \$hours,
	'minutes=i'   => \$minutes,
	'seconds=i'   => \$seconds,
	'm|message=s' => \$message,
	'h|help'      => \$help,
	'v|verbose'   => \$verbose,
);

eval {
	print "years=$years\n" if ($years && $verbose);
	print "months=$months\n" if ($months && $verbose);
	print "days=$days\n" if ($days && $verbose);
	print "hours=$hours\n" if ($hours && $verbose);
	print "minutes=$minutes\n" if ($minutes && $verbose);
	print "seconds=$seconds\n" if ($seconds && $verbose);

	usage() unless ($years || $months || $days || $hours || $minutes || $seconds);

	my $now = DateTime->now();
	my $time = DateTime->now();

	print "Start time: $now\n" if $verbose;

	$time = $time->add( years   => $years )   if $years;
	$time = $time->add( months  => $months )  if $months;
	$time = $time->add( days    => $days )    if $days;
	$time = $time->add( hours   => $hours )   if $hours;
	$time = $time->add( minutes => $minutes ) if $minutes;
	$time = $time->add( seconds => $seconds ) if $seconds;

	print "End time: $time\n" if $verbose;

	my $timediff_secs = $time->epoch() - $now->epoch();
	print "Sleeping for $timediff_secs seconds\n" if $verbose;
	sleep($timediff_secs);

	if (!$message) {
		$message = "countdown finished";
	}
	print "$message\n"; 
};
if ($@) {
	die $@;
}

exit;

sub usage {
	print "Usage:\n\n";
	print "user\@host> $0 [-seconds <secs>] [-minutes <mins>] [-hours <hrs>] [-days <days>] [-months <mnths>] [-years <yrs>] [-m <msg>] [-help] [-verbose]\n\n";
	die;
}
	


