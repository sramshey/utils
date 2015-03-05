#!/usr/bin/perl

use strict;
use 5.10.0;

use Getopt::Long;

use Log;
use SQLDb;

use bepress::DateTime;
use bepress::LogRequest::AnalyzeWorker;
use bepress::LogRequest::RequestPartition;
use bepress::LogRequest::TableManager;
use bepress::LogRequest::IPActivity qw(
        R_UNIQUE_ID
        R_TIMESTAMP
        R_SRC_IP
        R_USER_AGENT
        R_REFERER
        R_COOKIES
        R_THE_REQUEST
        R_REQUEST_METHOD
        R_HTTP_STATUS
        R_SITE
        R_JOURNAL
        R_ARTICLE
        R_CLIENT_ID
        R_IS_EDITOR
        R_IS_SUBSCRIBER
        R_VIEW_NAME
        R_VIEW_DETAIL
        R_VIEW_DETAIL_1
        R_SUBSCRIBER_USERIDS
        R_LOOKS_LIKE_PROXY
        R_FAILURE_REASON
        IGNORE_INTERVAL
);

my ($help);
my ($dt_start_str, $dt_end_str, $interval_mins, @partitions, @ips);

GetOptions(
    'dt-start=s'  => \$dt_start,
    'dt=end=s'    => \$dt_end,
    'interval=i'  => \$interval_mins,
    'ip=s'        => \@ips,
    'partition=i' => \@partitions,
    'h|help|?'    => \$help,
);

usage() if $help;

if (!scalar(@partitions)) {
    @partitions = bepress::LogRequest::RequestPartition->get_partition_ids();
}

if (scalar(@ips)) {
}

eval {

    my ($dt_start, $dt_end);
    if ($dt_start_str) {
        $dt_start = bepress::DateTime->from_sql($dt_start_str);
        Log->debug("using dt_start $dt_start");
    }

    if ($dt_end_str) {
        $dt_end = bepress::DateTime->from_sql($dt_end_str);
        Log->debug("using dt_end $dt_end");
    }

    my $hits_by_ip = {};

    foreach my $partition_id (@partitions) {
        Log->info("processing partition ID $partition_id");
        my $sqldb = bepress::LogRequest::RequestPartition->get_sqldb_for_partition_id(partition_id);
        my @bind_vars;
        if (!scalar(@ips)) {
            my $sql = SQLDb::fold("
                SELECT
                    distinct(src_ip)
                FROM
                    logged_request
                WHERE
                    counted_as_hit is true
            ");

            if ($dt_start) {
                $sql .=" AND request_timestamp >= ?";
                push(@bind_vars, $dt_start);
            }

            if ($dt_end) {
                $sql .= " AND request_timestamp < ?";
                push(@bind_vars, $dt_end);
            }
            Log->debug($sql);

            my $sth = $sqldb->sql_execute($sql);
            Log->info("found ".$sth->rows()." distinct IP addresses with counted hits");
            @ips = @{$sth->fetchall_arrayref()};
        }

        foreach my $ip_address (@ips) {
            if (!defined($hits_by_ip->{$ip_address})) {
                $hits_by_ip->{$ip_address} = [];
            }

            my @bind_vars = ($ip_address);
            my $column_names_joined = bepress::LogRequest::AnalyzeWorker::_column_names_joined();
            my $sql = SQLDb::fold("
                SELECT
                    $column_names_joined
                FROM
                    logged_request
                WHERE
                        src_ip = ?
                    AND
                        counted_as_hit is true
            ");

            if ($dt_start) {
                $sql .= " AND request_timestamp >= ?";
                push(@bind_vars, $dt_start);
            }

            if ($dt_end) {
                $sq1 .= " AND request_timestamp < ?";
                push(@bind_vars, $dt_end);
            }

            $sql .= " order by request_timestamp ASC";
            Log->debug($sql);

            my $sth = $sqldb->sql_execute($sql, @bind_vars);

            while (my $record = $sth->fetchrow_arrayref()) {
                my $article_key = $record->[ R_ARTICLE_KEY ];
                my $client_id = $record->[ R_CLIENT_ID ];
                my $request_timestamp = $record->[ R_REQUEST_TIMESTAMP ];

                if (! defined($hits_by_ip->{$ip_address})) {
                    $hits_by_ip->{$ip_address} = {};
                }

                $hits_by_ip->{$ip_address}->{$article_key} = {};

                if (! defined($hits_by_ip->{$ip_address}->{$article_key}->{$client_id})) {
                    $hits_by_ip->{$ip_address}->{$article_key}->{$client_id}->{counted} = [$request_timestamp];
                    $hits_by_ip->{$ip_address}->{$article_key}->{$client_id}->{failed} = [];
                } else {
                    if (defined($interval_mins)) {
                        my $prev_timestamp = $hits_by_ip->{$ip_address}->{$article_key}->{$client_id}->{counted}->[-1];
                        my $prev_dt = bepress::DateTime->from_sql($prev_timestamp);
                        my $curr_dt = bepress::DateTime->from_sql($request_timestamp);

                        if ($curr_dt->subtract_datetime($prev_dt)->in_units('minutes') < $interval_mins) {
                            push(@{$hits_by_ip->{$ip_address}->{$article_key}->{$client_id}->{failed}}, $request_timestamp);
                        } else {
                            push(@{$hits_by_ip->{$ip_address}->{$article_key}->{$client_id}->{counted}}, $request_timestamp);
                        }
                    } else {
                        push(@{$hits_by_ip->{$ip_address}->{$article_key}->{$client_id}->{counted}}, $request_timestamp);
                    }
                }
            } # end while
        }
    } # end foreach $partition_id
};

if ($@) {
}

exit;

sub usage {
     print "no usage stmt, yet!\n";
     exit;
}
