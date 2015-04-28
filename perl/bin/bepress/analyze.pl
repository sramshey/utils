#!/usr/bin/perl

use strict;
use 5.10.0;

use Getopt::Long;

use bepress::DateTime;
use bepress::LogRequest::AnalyzeWorkerNew;

my (
    $alternate_database,
    $batch_mode,
    $dt_start_str,
    $dt_end_str,
    @ips,
    $leave_pending_later_than,
    $look_back_interval_days,
    $max_rss_bytes,
    $partition,
    $record_dashboard_hit,
);

GetOptions(
    'alternate_database=s'       => \$alternate_database,
    'batch_mode'                 => \$batch_mode,
    'dt_start=s'                 => \$dt_start_str,
    'dt_end=s'                   => \$dt_end_str,
    'ip=s'                       => \@ips,
    'leave_pending_later_than=s' => \$leave_pending_later_than,
    'look_back_interval_days=i'  => \$look_back_interval_days,
    'max_rss_bytes=i'            => \$max_rss_bytes,
    'partition=i'                => \$partition,
    'record_dashboard_hit=i'     => \$record_dashboard_hit,
);

my %opts;

$opts{leave_pending_later_than} = $leave_pending_later_than if $leave_pending_later_than;
$opts{look_back_interval_days}  = $look_back_interval_days  if $look_back_interval_days;
$opts{max_rss_bytes} = $max_rss_bytes if $max_rss_bytes;
$opts{record_dashboard_hit} = 1 if $record_dashboard_hit;
$opts{batch_mode} = 1 if $batch_mode;
$opts{ips} = \@ips if scalar(@ips);

if ($dt_start_str) {
    my $dt_start = bepress::DateTime->from_sql($dt_start_str);
    $opts{dt_start} = $dt_start;
}

if ($dt_end_str) {
    my $dt_end = bepress::DateTime->from_sql($dt_end_str);
    $opts{dt_end} = $dt_end;
}

my $type = $batch_mode ? 'batch' : 'live';

print "creating $type worker...\n";
my $worker;

if ($batch_mode) {
    $worker = bepress::LogRequest::AnalyzeWorkerNew->new_batch_worker(%opts);
}
else {
    $worker = bepress::LogRequest::AnalyzeWorkerNew->new_live_worker(%opts);
}

print "running bepress::LogRequest::AnalyzeWorkerNew->run_once()\n";
$worker->run_once($partition);

print "finished\n";

exit;
