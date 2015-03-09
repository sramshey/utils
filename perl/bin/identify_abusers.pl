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
my ($dt_start_str, $dt_end_str, $exclude_all_clients, $interval_mins, @partitions, @ips);

GetOptions(
    'dt-start=s'  => \$dt_start_str,
    'dt=end=s'    => \$dt_end_str,
    'exclude-all-clients' => \$exclude_all_clients,
    'interval=i'  => \$interval_mins,
    'ip=s'        => \@ips,
    'partition=i' => \@partitions,
    'h|help|?'    => \$help,
);

usage() if $help;

if (!scalar(@partitions)) {
    Log->debug("no partition specified - getting partition IDs");
    @partitions = bepress::LogRequest::RequestPartition->get_partition_ids();
}

if (scalar(@ips)) {
    Log->debug("restricting IPs to ".join(',', @ips));
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

    if ($interval_mins) {
        Log->debug("using counter interval $interval_mins minutes");
    }
    

    my $hits_by_ip = {};

    foreach my $partition_id (@partitions) {
        Log->info("analyzing partition ID $partition_id");

        my $sqldb = bepress::LogRequest::RequestPartition->get_sqldb_for_partition_id($partition_id);
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

            my $sth = $sqldb->sql_execute($sql, @bind_vars);
            Log->info("found ".$sth->rows()." distinct IP addresses with counted hits");

            while (my ($ip_address) = $sth->fetchrow_array()) {
                push(@ips, $ip_address);
            }
        }

        foreach my $ip_address (@ips) {
            Log->debug("analyzing records for IP address $ip_address");

            if (!defined($hits_by_ip->{$ip_address})) {
                $hits_by_ip->{$ip_address} = {};
            }

            my @bind_vars = ($ip_address);
            my $column_names_joined = bepress::LogRequest::AnalyzeWorker::_column_names_joined();
            my $sql = SQLDb::fold("
                SELECT
                    $column_names_joined
                FROM
                    logged_request
                WHERE
                        src_ip=?
                    AND
                        counted_as_hit is true
            ");

            if ($dt_start) {
                $sql .= " AND request_timestamp >= ?";
                push(@bind_vars, $dt_start);
            }

            if ($dt_end) {
                $sql .= " AND request_timestamp < ?";
                push(@bind_vars, $dt_end);
            }

#            if (scalar(@ips)) {
#                Log->error("specifying IDs not yet supported");
#                die;
#                # this might not work....
#                $sql .= " AND src_ip IN (".join(',', @{'?'{(scalar(@ips))}}).")";
#                push(@bind_vars, @ips);
#            }

            $sql .= " order by request_timestamp ASC";
            Log->debug($sql);

            my $sth = $sqldb->sql_execute($sql, @bind_vars);

            while (my $record = $sth->fetchrow_arrayref()) {
                my $unique_id = $record->[ R_UNIQUE_ID ];
                my $article_id = $record->[ R_ARTICLE ];
                my $client_id = $record->[ R_CLIENT_ID ];
                my $request_timestamp = $record->[ R_TIMESTAMP ];

                Log->debug("unique ID: $unique_id\tarticle ID: $article_id\tclient ID: $client_id\trequest timestamp: $request_timestamp");

                if (! defined($hits_by_ip->{$ip_address}->{$article_id})) {
                    $hits_by_ip->{$ip_address}->{$article_id} = {};
                }

                $hits_by_ip->{$ip_address}->{$article_id} = {};

                if ($exclude_all_clients) {
                    if (! defined($hits_by_ip->{$ip_address}->{$article_id})) {
                        $hits_by_ip->{$ip_address}->{$article_id}->{counted} = [$unique_id];
                        $hits_by_ip->{$ip_address}->{$article_id}->{failed}  = [];
                    } else {
                        if (defined($interval_mins)) {
                            my $prev_timestamp = $hits_by_ip->{$ip_address}->{$article_id}->{counted}->[-1];
                            my $prev_dt = bepress::DateTime->from_sql($prev_timestamp);
                            my $curr_dt = bepress::DateTime->from_sql($request_timestamp);
                            
                            Log->debug("previous dt: $prev_dt");
                            Log->debug("current dt: $curr_dt");
    
                            if ($curr_dt->subtract_datetime($prev_dt)->in_units('minutes') < $interval_mins) {
                                Log->debug("failed");
                                push(@{$hits_by_ip->{$ip_address}->{$article_id}->{failed}}, $unique_id);
                            } else {
                                Log->debug("counted");
                                push(@{$hits_by_ip->{$ip_address}->{$article_id}->{counted}}, $unique_id);
                            }
                        } else {
                            Log->debug("counted");
                            push(@{$hits_by_ip->{$ip_address}->{$article_id}->{counted}}, $unique_id);
                        }
                    }
                } else {
                    if (! defined($hits_by_ip->{$ip_address}->{$article_id}->{$client_id})) {
                        # this is the first counted hit from this IP/article ID/client ID combo
                        $hits_by_ip->{$ip_address}->{$article_id}->{$client_id}->{counted} = [$unique_id];
                        $hits_by_ip->{$ip_address}->{$article_id}->{$client_id}->{failed}  = [];
                    } else {
                        if (defined($interval_mins)) {
                            my $prev_timestamp = $hits_by_ip->{$ip_address}->{$article_id}->{$client_id}->{counted}->[-1];
                            my $prev_dt = bepress::DateTime->from_sql($prev_timestamp);
                            my $curr_dt = bepress::DateTime->from_sql($request_timestamp);
                            
                            Log->debug("previous dt: $prev_dt");
                            Log->debug("current dt: $curr_dt");
    
                            if ($curr_dt->subtract_datetime($prev_dt)->in_units('minutes') < $interval_mins) {
                                Log->debug("failed");
                                push(@{$hits_by_ip->{$ip_address}->{$article_id}->{$client_id}->{failed}}, $unique_id);
                            } else {
                                Log->debug("counted");
                                push(@{$hits_by_ip->{$ip_address}->{$article_id}->{$client_id}->{counted}}, $unique_id);
                            }
                        }
                    }
                if (! defined($hits_by_ip->{$ip_address}->{$article_id}->{$client_id})) {
                    $hits_by_ip->{$ip_address}->{$article_id}->{$client_id} = { counted => ["$unique_id:$request_timestamp"], failed => [] };
                } else {
                    if (defined($interval_mins)) {
                        my $prev_hit = $hits_by_ip->{$ip_address}->{$article_id}->{$client_id}->{counted}->[-1];
                        my ($prev_hit_id, $prev_hit_epoch) = split(':', $prev_hit);
                        my $prev_dt = bepress::DateTime->from_epoch(epoch => $prev_hit_epoch);
                        my $curr_dt = bepress::DateTime->from_epoch(epoch => $request_timestamp);

                        Log->debug("previous dt: $prev_dt");
                        Log->debug("current dt: $curr_dt");

                        my $diff_minutes = abs($curr_dt->epoch() - $prev_dt->epoch()) / 60;
                        Log->debug("time diff (mins): $diff_minutes");

                        if ($diff_minutes < $interval_mins) {
                            Log->debug("failed $interval_mins minute interval");
                            push(@{$hits_by_ip->{$ip_address}->{$article_id}->{$client_id}->{failed}}, "$unique_id:$request_timestamp");
                        } else {
                            Log->debug("counted ok");
                            push(@{$hits_by_ip->{$ip_address}->{$article_id}->{$client_id}->{counted}}, "$unique_id:$request_timestamp");
                        }
                    }
                }
            } # end while
            Log->debug("finished analyzing IP address $ip_address");
        } # end foreach $ip_address

        Log->info("updating database records");

        foreach my $ip_address (keys %$hits_by_ip) {
            Log->trace("updating IP address: $ip_address");

            foreach my $article_id (keys %{$hits_by_ip->{$ip_address}}) {
                Log->trace("updating article ID $article_id");

                if ($exclude_all_clients) {
                    #code
                } else {}

                foreach my $client_id (keys %{$hits_by_ip->{$ip_address}->{$article_id}}) {
                    Log->debug("ip address: $ip_address\tarticle ID: $article_id\tclient ID: $client_id");

                    foreach my $key (@{$hits_by_ip->{$ip_address}->{$article_id}->{$client_id}->{failed}}) {
                        my ($unique_id, $request_timestamp) = split(':', $key);
                        my $sql = SQLDb::fold("
                            UPDATE
                                logged_request
                            SET
                                counted_as_hit = false,
                                failure_reason = 'failed ".$interval_mins." minute interval'
                            WHERE
                                unique_id = ?
                        ");
                        my @bind_vars = ($unique_id);
                        Log->trace($sql);
                        my $rows_updated = $sqldb->sql_do($sql, {}, @bind_vars);
                        if (!$rows_updated) {
                            Log->error("failed to update record ID $unique_id! dying");
                            exit(1);
                        }
                        Log->trace("$rows_updated row updated");
                    } # end foreach $unique_id
                    Log->trace("finished updating client ID: $client_id");

                } # end foreach $client_id
                Log->trace("finished updating article ID $article_id");

            } # end foreach $article_id
            Log->trace("finished updating IP address $ip_address");

        } # end foreach $ip_address
        Log->info("finished with partition $partition_id");
        
    } # end foreach $partition_id
    Log->info("finished with all partitions");

    Log->info("program complete");
};

if ($@) {
    Log->error($@);
    exit(1);
}

exit;

sub usage {
     print "no usage stmt, yet!\n";
     exit;
}
