var level = require('level');
var sublevel = require('subleveldown');
var changes = require('changes-feed');
var changesdown = require('changesdown');
var isarray = require('isarray');
var crypto = require('crypto');
var Ix = require('../');

var minimist = require('minimist');
var argv = minimist(process.argv.slice(2));

var up = level('test.db', { valueEncoding: 'json' });
var feed = changes(sublevel(up, 'feed'));
var db = changesdown(sublevel(up, 'db'), feed, { valueEncoding: 'json' });

var indexes = Ix({
    ixdb: level('index.db', { valueEncoding: 'json' }),
    chdb: db,
    feed: feed
});
indexes.add('user', function (row, cb) {
    if (/^user!/.test(row.key)) {
        cb(null, {
            name: row.value.name,
            space: row.value.hackerspace
        });
    }
    else cb()
});

if (argv._[0] === 'create') {
    var id = crypto.randomBytes(16).toString('hex');
    var name = argv._[1], space = argv._[2];
    var value = { name: name, hackerspace: space };
    
    indexes.exists('user.name', name, function (err, ex) {
        if (err) return console.error(err);
        if (ex) return console.error('name in use');
        
        db.put('user!' + id, value, function (err) {
            if (err) console.error(err);
        });
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
