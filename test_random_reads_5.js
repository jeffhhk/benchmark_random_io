const fs = require("fs");
const { Buffer } = require('node:buffer');
const process = require('process')
const { argv } = require('node:process');

var usage = function() {
    console.log("*** usage ****");
    console.log("   node test_random_reads_1.js <degree of parallelism> <read only large file>")
}

var run = function(numPhases, relf, iMax, done) {
    var fds = Array(numPhases).fill('junk',0,numPhases).map(junk => {
        var fd = fs.openSync(relf,'r')
        // TODO handle file access errors
        var buf = Buffer.alloc(1024);
        return [fd,buf];
    })
    var t0 = new Date();
    return doreads(numPhases, fds, t0, iMax, done);
}

var doreads = function(numPhases, fds, t0, iMax, done) {
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
            var iPos = Math.floor(Math.random() * 107374182400); // TODO: measure file
            // console.log({event:"fs.read", i, j, numOut})
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

if(argv.length <= 3) {
    usage()
    process.exit(1)
} else {
    var iMax = 100000
    var pargs = parseArgs(argv)
    run(pargs[0], pargs[1], iMax, function(dt) {
        var iops = iMax/(dt/1000.0)
        console.log({event:"run completed", dt, iops})
    })
}
