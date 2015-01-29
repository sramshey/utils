#!/usr/bin/perl

=begin

Given a journal context key and a datetime boundary, this script will read
the counted downloads from all 4 logged_request partitions and send the
corresponding hit messages to the record_download queue. This should end with
the hits being tallied in the articles_1 database.

=cut

use 5.10.1;
use strict;

use Data::Dumper;
use File::Slurp;
use Getopt::Long;
use Time::HiRes;

use bepress::DateTime;
use bepress::LogRequest::AnalyzeWorker;
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
    );

use Context;
use Log;
use SQLDb;

my (@context_keys, $file, $dt_start_str, $dt_end_str);
my $help;

GetOptions(
    'k|key|keys=s' => \@context_keys,
    'f|file=s'     => \$file,
    'dt-start=s'   => \$dt_start_str,
    'dt-end=s'     => \$dt_end_str,
    'h|help|?'     => \$help,
);

if (!@context_keys && !$file) {
    print "you must specify at least one context key or input file!";
    usage();
}

eval {

    if ($file) {
        if (! -f $file) {
            die "unable to locate specified input file $file!";
        }
        Log->debug("reading context keys from input file $file");
        @context_keys = read_file($file, chomp => 1);
    }

    Log->debug("Looking up ".scalar(@context_keys)." context".scalar(@context_keys) > 1 ? "s" : "");

    my $dt_start;
    if ($dt_start_str) {
        $dt_start = bepress::DateTime->from_sql($dt_start_str);
    }

    my $dt_end;
    if ($dt_end_str) {
        $dt_end = bepress::DateTime->from_sql($dt_end_str);
    } else {
        Log->debug("using current datetime as dt_end");
        $dt_end = bepress::DateTime->now();
    }

    my $sql_dt_formatter = bepress::DateTime::get_sql_format_class();

    foreach my $partition (1,2,3,4) {
        my $analyze_worker = bepress::LogRequest::AnalyzeWorker->new_batch_worker(
            batch_mode => 1,
            dt_start   => $dt_start,
            dt_end     => $dt_end,
            partition  => $partition,
        );
        my $dbh = SQLDb->get(PROFILE => "logged_request_activity_$partition");
    
        foreach my $context_key (@context_keys) {
            my $context;
            eval {
                $context = Context->get($context_key);
            };
            if ($@) {
                Log->error("Failed lookup on context key $context_key");
                next;
            }
    
            if (!$context->isa('Journal')) {
                Log->error("context key $context_key does not correspond to a journal!");
                next;
            }
    
            my @bind_vars = ($context_key, $sql_dt_formatter->format_datetime($dt_end));
            my $column_names_joined = bepress::LogRequest::AnalyzeWorker::_column_names_joined();
            my $sql = SQLDb::fold("
                SELECT
                    $column_names_joined
                FROM
                    logged_request
                WHERE
                        journal_key = ?
                    AND
                        is_analyzed IS TRUE
                    AND
                        counted_as_hit IS TRUE
                    AND
                        request_timestamp < ?
            ");
    
            if ($dt_start) {
                $sql .= " AND request_timestamp >= ?";
                push(@bind_vars, $sql_dt_formatter->format_datetime($dt_start));
            }
    
            $sql .= " ORDER BY request_timestamp ASC";
            Log->debug("pending_sql: $sql");
    
            my $time_start = [Time::HiRes::gettimeofday()];
    
            my $sth = $dbh->sql_execute($sql, @bind_vars);
    
            my $time_end = [Time::HiRes::gettimeofday()];
            my $elapsed = Time::HiRes::tv_interval($time_start, $time_end);
    
            Log->debug("got ".$sth->rows()." logged_request records");

            Log->debug("sending article hits");
            while ( my @record = $sth->fetchrow_array() ) {
                $analyze_worker->_send_article_hit(\@record);
                Log->debug("finished sending hit record ID $record[ R_UNIQUE_ID ]");
            }
        } # end foreach $context_key

        Log->info("Finished with partition $partition");
    } # end foreach $partition

    Log->info("program complete");
};
if ($@) {
    die $@;
}


exit;


sub usage {
    print "\nUsage:\n\n".
          "    user\@host> $0 --key=<context_key> [--key=<context_key>]\n\nor\n".
          "    user\@host> $0 --file=<file>\n\n";
    exit;
}
