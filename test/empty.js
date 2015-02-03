var level = require('level-test')();
var sublevel = require('subleveldown');
var through = require('through2');
var changes = require('changes-feed');
var changesdown = require('changesdown');
var chi = require('../');
var test = require('tape');

function name (x) {
    return x + Math.floor(Math.pow(16,8)*Math.random()).toString(16);
}

test('empty index', function (t) {
    t.plan(1);
    var up = level(name('main.db'), { valueEncoding: 'json' });
    var feed = changes(sublevel(up, 'feed'));
    var db = changesdown(sublevel(up, 'db'), feed, { valueEncoding: 'json' });
    
    var indexes = chi({
        ixdb: level(name('index.db'), { valueEncoding: 'json' }),
        chdb: db,
        feed: feed
    });
    indexes.createReadStream('user.name').pipe(collect(function (rows) {
        t.deepEqual(rows, []);
    }));
});

function collect (cb) {
    var rows = [];
    return through.obj(write, end);
    function write (row, enc, next) { rows.push(row); next() }
    function end () { cb(rows) }
}
