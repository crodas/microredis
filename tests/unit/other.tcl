start_server {tags {"other"}} {
    if {$::force_failure} {
        # This is used just for test suite development purposes.
        test {Failing test} {
            format err
        } {ok}
    }

    test {SAVE - make sure there are all the types as values} {
        # Wait for a background saving in progress to terminate
        waitForBgsave r
        r lpush mysavelist hello
        r lpush mysavelist world
        r set myemptykey {}
        r set mynormalkey {blablablba}
        r zadd mytestzset 10 a
        r zadd mytestzset 20 b
        r zadd mytestzset 30 c
        r save
    } {OK} {needs:save}

    tags {slow} {
        if {$::accurate} {set iterations 10000} else {set iterations 1000}
        foreach fuzztype {binary alpha compr} {
            test "FUZZ stresser with data model $fuzztype" {
                set err 0
                for {set i 0} {$i < $iterations} {incr i} {
                    set fuzz [randstring 0 512 $fuzztype]
                    r set foo $fuzz
                    set got [r get foo]
                    if {$got ne $fuzz} {
                        set err [list $fuzz $got]
                        break
                    }
                }
                set _ $err
            } {0}
        }
    }

    test {BGSAVE} {
        waitForBgsave r
        r flushdb
        r save
        r set x 10
        r bgsave
        waitForBgsave r
        r debug reload
        r get x
    } {10} {needs:save}

    test {SELECT an out of range DB} {
        catch {r select 1000000} err
        set _ $err
    } {*index is out of range*} {cluster:skip}

    test {EXPIRES after a reload (snapshot + append only file rewrite)} {
        r flushdb
        r set x 10
        r expire x 1000
        r save
        r debug reload
        set ttl [r ttl x]
        set e1 [expr {$ttl > 900 && $ttl <= 1000}]
        r bgrewriteaof
        waitForBgrewriteaof r
        r debug loadaof
        set ttl [r ttl x]
        set e2 [expr {$ttl > 900 && $ttl <= 1000}]
        list $e1 $e2
    } {1 1} {needs:debug needs:save}

    test {EXPIRES after AOF reload (without rewrite)} {
        r flushdb
        r config set appendonly yes
        r config set aof-use-rdb-preamble no
        r set x somevalue
        r expire x 1000
        r setex y 2000 somevalue
        r set z somevalue
        r expireat z [expr {[clock seconds]+3000}]

        # Milliseconds variants
        r set px somevalue
        r pexpire px 1000000
        r psetex py 2000000 somevalue
        r set pz somevalue
        r pexpireat pz [expr {([clock seconds]+3000)*1000}]

        # Reload and check
        waitForBgrewriteaof r
        # We need to wait two seconds to avoid false positives here, otherwise
        # the DEBUG LOADAOF command may read a partial file.
        # Another solution would be to set the fsync policy to no, since this
        # prevents write() to be delayed by the completion of fsync().
        after 2000
        r debug loadaof
        set ttl [r ttl x]
        assert {$ttl > 900 && $ttl <= 1000}
        set ttl [r ttl y]
        assert {$ttl > 1900 && $ttl <= 2000}
        set ttl [r ttl z]
        assert {$ttl > 2900 && $ttl <= 3000}
        set ttl [r ttl px]
        assert {$ttl > 900 && $ttl <= 1000}
        set ttl [r ttl py]
        assert {$ttl > 1900 && $ttl <= 2000}
        set ttl [r ttl pz]
        assert {$ttl > 2900 && $ttl <= 3000}
        r config set appendonly no
    } {OK} {needs:debug}

    test {APPEND basics} {
        r del foo
        list [r append foo bar] [r get foo] \
             [r append foo 100] [r get foo]
    } {3 bar 6 bar100}

    test {APPEND basics, integer encoded values} {
        set res {}
        r del foo
        r append foo 1
        r append foo 2
        lappend res [r get foo]
        r set foo 1
        r append foo 2
        lappend res [r get foo]
    } {12 12}

    test {APPEND fuzzing} {
        set err {}
        foreach type {binary alpha compr} {
            set buf {}
            r del x
            for {set i 0} {$i < 1000} {incr i} {
                set bin [randstring 0 10 $type]
                append buf $bin
                r append x $bin
            }
            if {$buf != [r get x]} {
                set err "Expected '$buf' found '[r get x]'"
                break
            }
        }
        set _ $err
    } {}

    # Leave the user with a clean DB before to exit
    test {FLUSHDB} {
        set aux {}
        if {$::singledb} {
            r flushdb
            lappend aux 0 [r dbsize]
        } else {
            r select 9
            r flushdb
            lappend aux [r dbsize]
            r select 10
            r flushdb
            lappend aux [r dbsize]
        }
    } {0 0}

    test {Perform a final SAVE to leave a clean DB on disk} {
        waitForBgsave r
        r save
    } {OK} {needs:save}

    test {RESET clears and discards MULTI state} {
        r multi
        r set key-a a

        r reset
        catch {r exec} err
        assert_match {*EXEC without MULTI*} $err
    } {} {needs:reset}

    test {RESET clears Pub/Sub state} {
        r subscribe channel-1
        r reset

        # confirm we're not subscribed by executing another command
        r set key val
    } {OK} {needs:reset}
}

start_server {tags {"other external:skip"}} {
    test {Don't rehash if redis has child proecess} {
        r config set save ""
        r config set rdb-key-save-delay 1000000

        populate 4096 "" 1
        r bgsave
        wait_for_condition 10 100 {
            [s rdb_bgsave_in_progress] eq 1
        } else {
            fail "bgsave did not start in time"
        }

        r mset k1 v1 k2 v2
        # Hash table should not rehash
        assert_no_match "*table size: 8192*" [r debug HTSTATS 9]
        exec kill -9 [get_child_pid 0]
        after 200

        # Hash table should rehash since there is no child process,
        # size is power of two and over 4098, so it is 8192
        r set k3 v3
        assert_match "*table size: 8192*" [r debug HTSTATS 9]
    } {} {needs:local-process}
}

proc read_proc_title {pid} {
    set fd [open "/proc/$pid/cmdline" "r"]
    set cmdline [read $fd 1024]
    close $fd

    return $cmdline
}

start_server {tags {"other external:skip"}} {
    test {Process title set as expected} {
        # Test only on Linux where it's easy to get cmdline without relying on tools.
        # Skip valgrind as it messes up the arguments.
        set os [exec uname]
        if {$os == "Linux" && !$::valgrind} {
            # Set a custom template
            r config set "proc-title-template" "TEST {title} {listen-addr} {port} {tls-port} {unixsocket} {config-file}"
            set cmdline [read_proc_title [srv 0 pid]]

            assert_equal "TEST" [lindex $cmdline 0]
            assert_match "*/redis-server" [lindex $cmdline 1]
            
            if {$::tls} {
                set expect_port 0
                set expect_tls_port [srv 0 port]
            } else {
                set expect_port [srv 0 port]
                set expect_tls_port 0
            }
            set port [srv 0 port]

            assert_equal "$::host:$port" [lindex $cmdline 2]
            assert_equal $expect_port [lindex $cmdline 3]
            assert_equal $expect_tls_port [lindex $cmdline 4]
            assert_match "*/tests/tmp/server.*/socket" [lindex $cmdline 5]
            assert_match "*/tests/tmp/redis.conf.*" [lindex $cmdline 6]

            # Try setting a bad template
            catch {r config set "proc-title-template" "{invalid-var}"} err
            assert_match {*template format is invalid*} $err
        }
    }
}

