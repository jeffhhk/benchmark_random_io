/*
Demonstrate random io performance benefit of io_uring in libuv.
    https://github.com/libuv/libuv/issues/1947

Versions pertaining to where io_uring support was added:
    https://github.com/libuv/libuv/commit/d2c31f429b87b476a7f1344d145dad4752a406d4
         v1.48.0
        …
         v1.45.0
        bnoordhuis committed on Apr 18, 2023
    https://github.com/libuv/libuv/releases/tag/v1.45.0
        v1.45.0: 2023.05.19, Version 1.45.0 (Stable)
        @santigimeno santigimeno released this May 19, 2023
        · 137 commits to v1.x since this release
         v1.45.0
         96e0554
    https://github.com/nodejs/node/blob/main/doc/changelogs/CHANGELOG_V20.md#2023-06-08-version-2030-current-targos
        2023-06-08, Version 20.3.0 (Current), @targos
        Notable Changes
        [bfcb3d1d9a] - deps: upgrade to libuv 1.45.0, including significant performance improvements to file system operations on Linux (Santiago Gimeno) #48078

Intended configuration is to use a file that is 2x larger than system RAM.  Measure your RAM and create the file with dd yourself.  E.g. for 16 GB system:

    dd bs=$((1024*1024)) count=32000 if=/dev/zero of=test.tmp
    node 10 test.tmp

Example measurements on node 20.11.1:
    2021 consumer SSD Ubuntu 20:
        5x faster with 10 file handles than with 1 (31705 vs 6843)
        10x faster with 100 file handles than with 1 (62932 vs 6843)
    2014 macbook pro, macOS 10.14.6:
        2.4x faster with 10 file handles than with 1 (7001 vs 2927).

*/

const fs = require("fs");
const process = require('process')
const { Buffer } = require('node:buffer');

var usage = function() {
    console.log("*** usage ****");
    console.log("   node test_random_reads_1.js <degree of parallelism> <read only large file>")
}

var run = function(numPhases, relf, iMax, done) {
    var stat = fs.statSync(relf);
    var fds = Array(numPhases).fill('junk',0,numPhases).map(junk => {
        var fd = fs.openSync(relf,'r')
        // TODO handle file access errors
        var buf = Buffer.alloc(1024);
        return [fd,buf];
    })
    var t0 = new Date();
    return doreads(numPhases, fds, t0, iMax, stat.size, done);
}

var doreads = function(numPhases, fds, t0, iMax, iPosMax, done) {
    var numOut = 0
    var loop = function(i, j) {
        if(i <= 0) {
            var dt = new Date() - t0;
            return done(dt);
        }
        if(j >= 0) {
            // console.log({event:"unpacking", j})
            var fd = fds[j][0]
            var buf = fds[j][1]
            var iPos = Math.floor(Math.random() * iPosMax);
            // console.log({event:"fs.read", i, j, numOut, iPos})
            fs.read(fd, buf, 0, 1024, iPos, function(err, bytesRead, buf) {
                numOut -= 1
                if(numOut == 0) {
                    return loop(i-1, numPhases-1);
                }
            });
            numOut += 1
            return loop(i, j-1)
        }
    }
    return loop(Math.round(iMax/numPhases), numPhases-1)
}

var parseArgs = function(argv) {
    var relf = argv[3]
    if(! fs.existsSync(relf)) {
        throw("file not found: "+relf)
    }
    var numPhases = parseInt(argv[2])
    if(typeof numPhases!='number' || isNaN(numPhases)) {
        throw("could not parse number: "+argv[2])
    }
    return [numPhases, relf]
}

if(process.argv.length <= 3) {
    usage()
    process.exit(1)
} else {
    var iMax = 100000
    var pargs = parseArgs(process.argv)
    run(pargs[0], pargs[1], iMax, function(dt) {
        var iops = iMax/(dt/1000.0)
        console.log({event:"run completed", dt, iops})
    })
}
