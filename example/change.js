var level = require('level');
var sublevel = require('subleveldown');
var through = require('through2');
var changes = require('changes-feed');
var changesdown = require('changesdown');
var chindexer = require('../');

var minimist = require('minimist');
var argv = minimist(process.argv.slice(2));

var up = level('/tmp/test.db', { valueEncoding: 'json' });
var feed = changes(sublevel(up, 'feed'));
var db = changesdown(sublevel(up, 'db'), feed, { valueEncoding: 'json' });

var indexes = chindexer({
    ixdb: level('/tmp/index.db', { valueEncoding: 'json' }),
    chdb: db,
    feed: feed
});
indexes.add(function (row, cb) {
    if (/^user!/.test(row.key)) {
        cb(null, {
            'user.name': row.value.name,
            'user.space': row.value.hackerspace
        });
    }
    else cb()
});

if (argv._[0] === 'create') {
    var id = require('crypto').randomBytes(16).toString('hex');
    var name = argv._[1], space = argv._[2];
    var value = { name: name, hackerspace: space };
    
    userExists(name, function (err, ex) {
        if (err) return console.error(err);
        if (ex) return console.error('name in use');
        
        db.put('user!' + id, value, function (err) {
            if (err) console.error(err);
        });
    });
}
else if (argv._[0] === 'clear') {
    indexes.clear(argv._[1], function (err) {
        if (err) console.error(err);
    });
}
else if (argv._[0] === 'by-name') {
    indexes.createReadStream('user.name', argv)
        .on('data', console.log)
    ;
}
else if (argv._[0] === 'by-space') {
    indexes.createReadStream('user.space', argv)
        .on('data', console.log)
    ;
}

function userExists (name, cb) {
    indexes.createReadStream('user.name', name, { gte: name, lte: name })
        .pipe(through.obj(write, end))
    ;
    function write (row, enc, next) { cb(null, true) }
    function end () { cb(null, false) }
}
