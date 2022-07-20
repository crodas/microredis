proc wait_for_dbsize {size} {
    set r2 [redis_client]
    wait_for_condition 50 100 {
        [$r2 dbsize] == $size
    } else {
        fail "Target dbsize not reached"
    }
    $r2 close
}

start_server {tags {"multi"}} {
    test {MUTLI / EXEC basics} {
        r del mylist
        r rpush mylist a
        r rpush mylist b
        r rpush mylist c
        r multi
        set v1 [r lrange mylist 0 -1]
        set v2 [r ping]
        set v3 [r exec]
        list $v1 $v2 $v3
    } {QUEUED QUEUED {{a b c} PONG}}

    test {DISCARD} {
        r del mylist
        r rpush mylist a
        r rpush mylist b
        r rpush mylist c
        r multi
        set v1 [r del mylist]
        set v2 [r discard]
        set v3 [r lrange mylist 0 -1]
        list $v1 $v2 $v3
    } {QUEUED OK {a b c}}

    test {Nested MULTI are not allowed} {
        set err {}
        r multi
        catch {[r multi]} err
        r exec
        set _ $err
    } {*ERR MULTI*}

    test {MULTI where commands alter argc/argv} {
        r sadd myset a
        r multi
        r spop myset
        list [r exec] [r exists myset]
    } {a 0}

    test {WATCH inside MULTI is not allowed} {
        set err {}
        r multi
        catch {[r watch x]} err
        r exec
        set _ $err
    } {*ERR WATCH*}

    test {EXEC fails if there are errors while queueing commands #1} {
        r del foo1{t} foo2{t}
        r multi
        r set foo1{t} bar1
        catch {r non-existing-command}
        r set foo2{t} bar2
        catch {r exec} e
        assert_match {EXECABORT*} $e
        list [r exists foo1{t}] [r exists foo2{t}]
    } {0 0}

    test {If EXEC aborts, the client MULTI state is cleared} {
        r del foo1{t} foo2{t}
        r multi
        r set foo1{t} bar1
        catch {r non-existing-command}
        r set foo2{t} bar2
        catch {r exec} e
        assert_match {EXECABORT*} $e
        r ping
    } {PONG}

    test {EXEC works on WATCHed key not modified} {
        r watch x{t} y{t} z{t}
        r watch k{t}
        r multi
        r ping
        r exec
    } {PONG}

    test {EXEC fail on WATCHed key modified (1 key of 1 watched)} {
        r set x 30
        r watch x
        r set x 40
        r multi
        r ping
        r exec
    } {}

    test {EXEC fail on WATCHed key modified (1 key of 5 watched)} {
        r set x{t} 30
        r watch a{t} b{t} x{t} k{t} z{t}
        r set x{t} 40
        r multi
        r ping
        r exec
    } {}

    test {EXEC fail on lazy expired WATCHed key} {
        r flushall
        r debug set-active-expire 0

        r del key
        r set key 1 px 2
        r watch key

        after 100

        r multi
        r incr key
        assert_equal [r exec] {}
        r debug set-active-expire 1
    } {OK} {needs:debug}

    test {After successful EXEC key is no longer watched} {
        r set x 30
        r watch x
        r multi
        r ping
        r exec
        r set x 40
        r multi
        r ping
        r exec
    } {PONG}

    test {After failed EXEC key is no longer watched} {
        r set x 30
        r watch x
        r set x 40
        r multi
        r ping
        r exec
        r set x 40
        r multi
        r ping
        r exec
    } {PONG}

    test {It is possible to UNWATCH} {
        r set x 30
        r watch x
        r set x 40
        r unwatch
        r multi
        r ping
        r exec
    } {PONG}

    test {UNWATCH when there is nothing watched works as expected} {
        r unwatch
    } {OK}

    test {FLUSHALL is able to touch the watched keys} {
        r set x 30
        r watch x
        r flushall
        r multi
        r ping
        r exec
    } {}

    test {FLUSHALL does not touch non affected keys} {
        r del x
        r watch x
        r flushall
        r multi
        r ping
        r exec
    } {PONG}

    test {FLUSHDB is able to touch the watched keys} {
        r set x 30
        r watch x
        r flushdb
        r multi
        r ping
        r exec
    } {}

    test {FLUSHDB does not touch non affected keys} {
        r del x
        r watch x
        r flushdb
        r multi
        r ping
        r exec
    } {PONG}

    test {WATCH is able to remember the DB a key belongs to} {
        r select 5
        r set x 30
        r watch x
        r select 1
        r set x 10
        r select 5
        r multi
        r ping
        set res [r exec]
        # Restore original DB
        r select 9
        set res
    } {PONG} {singledb:skip}

    test {WATCH will consider touched keys target of EXPIRE} {
        r del x
        r set x foo
        r watch x
        r expire x 10
        r multi
        r ping
        r exec
    } {}

    test {WATCH will consider touched expired keys} {
        r flushall
        r del x
        r set x foo
        r expire x 1
        r watch x

        # Wait for the keys to expire.
        wait_for_dbsize 0

        r multi
        r ping
        r exec
    } {}

    test {DISCARD should clear the WATCH dirty flag on the client} {
        r watch x
        r set x 10
        r multi
        r discard
        r multi
        r incr x
        r exec
    } {11}

    test {DISCARD should UNWATCH all the keys} {
        r watch x
        r set x 10
        r multi
        r discard
        r set x 10
        r multi
        r incr x
        r exec
    } {11}

    test {MULTI / EXEC is propagated correctly (single write command)} {
        set repl [attach_to_replication_stream]
        r multi
        r set foo bar
        r exec
        assert_replication_stream $repl {
            {select *}
            {multi}
            {set foo bar}
            {exec}
        }
        close_replication_stream $repl
    } {} {needs:repl}

    test {MULTI / EXEC is propagated correctly (empty transaction)} {
        set repl [attach_to_replication_stream]
        r multi
        r exec
        r set foo bar
        assert_replication_stream $repl {
            {select *}
            {set foo bar}
        }
        close_replication_stream $repl
    } {} {needs:repl}

    test {MULTI / EXEC is propagated correctly (read-only commands)} {
        r set foo value1
        set repl [attach_to_replication_stream]
        r multi
        r get foo
        r exec
        r set foo value2
        assert_replication_stream $repl {
            {select *}
            {set foo value2}
        }
        close_replication_stream $repl
    } {} {needs:repl}

    test {MULTI / EXEC is propagated correctly (write command, no effect)} {
        r del bar
        r del foo
        set repl [attach_to_replication_stream]
        r multi
        r del foo
        r exec

        # add another command so that when we see it we know multi-exec wasn't
        # propagated
        r incr foo

        assert_replication_stream $repl {
            {select *}
            {incr foo}
        }
        close_replication_stream $repl
    } {} {needs:repl}

    test {DISCARD should not fail during OOM} {
        set rd [redis_deferring_client]
        $rd config set maxmemory 1
        assert  {[$rd read] eq {OK}}
        r multi
        catch {r set x 1} e
        assert_match {OOM*} $e
        r discard
        $rd config set maxmemory 0
        assert  {[$rd read] eq {OK}}
        $rd close
        r ping
    } {PONG} {needs:config-maxmemory}

    test {exec with write commands and state change} {
        # check that exec that contains write commands fails if server state changed since they were queued
        set r1 [redis_client]
        r set xx 1
        r multi
        r incr xx
        $r1 config set min-replicas-to-write 2
        catch {r exec} e
        assert_match {*EXECABORT*NOREPLICAS*} $e
        set xx [r get xx]
        # make sure that the INCR wasn't executed
        assert { $xx == 1}
        $r1 config set min-replicas-to-write 0
        $r1 close
    } {0} {needs:repl}

    test {exec with read commands and stale replica state change} {
        # check that exec that contains read commands fails if server state changed since they were queued
        r config set replica-serve-stale-data no
        set r1 [redis_client]
        r set xx 1

        # check that GET is disallowed on stale replica, even if the replica becomes stale only after queuing.
        r multi
        r get xx
        $r1 replicaof localhsot 0
        catch {r exec} e
        assert_match {*EXECABORT*MASTERDOWN*} $e

        # check that PING is allowed
        r multi
        r ping
        $r1 replicaof localhsot 0
        set pong [r exec]
        assert {$pong == "PONG"}

        # check that when replica is not stale, GET is allowed
        # while we're at it, let's check that multi is allowed on stale replica too
        r multi
        $r1 replicaof no one
        r get xx
        set xx [r exec]
        # make sure that the INCR was executed
        assert { $xx == 1 }
        $r1 close
    } {0} {needs:repl cluster:skip}

    test {EXEC with only read commands should not be rejected when OOM} {
        set r2 [redis_client]

        r set x value
        r multi
        r get x
        r ping

        # enforcing OOM
        $r2 config set maxmemory 1

        # finish the multi transaction with exec
        assert { [r exec] == {value PONG} }

        # releasing OOM
        $r2 config set maxmemory 0
        $r2 close
    } {0} {needs:config-maxmemory}

    test {EXEC with at least one use-memory command should fail} {
        set r2 [redis_client]

        r multi
        r set x 1
        r get x

        # enforcing OOM
        $r2 config set maxmemory 1

        # finish the multi transaction with exec
        catch {r exec} e
        assert_match {EXECABORT*OOM*} $e

        # releasing OOM
        $r2 config set maxmemory 0
        $r2 close
    } {0} {needs:config-maxmemory}

    test {MULTI propagation of PUBLISH} {
        set repl [attach_to_replication_stream]

        # make sure that PUBLISH inside MULTI is propagated in a transaction
        r multi
        r publish bla bla
        r exec

        assert_replication_stream $repl {
            {select *}
            {multi}
            {publish bla bla}
            {exec}
        }
        close_replication_stream $repl
    } {} {needs:repl cluster:skip}

    test {MULTI propagation of SCRIPT LOAD} {
        set repl [attach_to_replication_stream]

        # make sure that SCRIPT LOAD inside MULTI is propagated in a transaction
        r multi
        r script load {redis.call('set', KEYS[1], 'foo')}
        set res [r exec]
        set sha [lindex $res 0]

        assert_replication_stream $repl {
            {select *}
            {multi}
            {script load *}
            {exec}
        }
        close_replication_stream $repl
    } {} {needs:repl}

    test {MULTI propagation of SCRIPT LOAD} {
        set repl [attach_to_replication_stream]

        # make sure that EVAL inside MULTI is propagated in a transaction
        r config set lua-replicate-commands no
        r multi
        r eval {redis.call('set', KEYS[1], 'bar')} 1 bar
        r exec

        assert_replication_stream $repl {
            {select *}
            {multi}
            {eval *}
            {exec}
        }
        close_replication_stream $repl
    } {} {needs:repl}

    tags {"stream"} {
        test {MULTI propagation of XREADGROUP} {
            # stream is a special case because it calls propagate() directly for XREADGROUP
            set repl [attach_to_replication_stream]

            r XADD mystream * foo bar
            r XGROUP CREATE mystream mygroup 0

            # make sure the XCALIM (propagated by XREADGROUP) is indeed inside MULTI/EXEC
            r multi
            r XREADGROUP GROUP mygroup consumer1 STREAMS mystream ">"
            r exec

            assert_replication_stream $repl {
                {select *}
                {xadd *}
                {xgroup CREATE *}
                {multi}
                {xclaim *}
                {exec}
            }
            close_replication_stream $repl
        } {} {needs:repl}
    }

}
