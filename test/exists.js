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

var expected = [
    { type: 'put', key: 'a', value: 123, exists: false },
    { type: 'put', key: 'b', value: 555, exists: false },
    { type: 'put', key: 'a', value: 444, exists: true },
    { type: 'put', key: 'c', value: 1000, exists: false },
    { type: 'del', key: 'b', exists: true }
];

test('exists', function (t) {
    t.plan(expected.length + 2);
    var up = level(name('main.db'), { valueEncoding: 'json' });
    var feed = changes(sublevel(up, 'feed'));
    var db = changesdown(sublevel(up, 'db'), feed, { valueEncoding: 'json' });
    
    var indexes = chi({
        ixdb: level(name('index.db'), { valueEncoding: 'json' }),
        chdb: db,
        feed: feed
    });
    
    var count = 0;
    indexes.add(function (row, cb) {
        t.deepEqual(relevant(row), expected.shift());
        if (row.type === 'put') {
            count += row.exists ? 0 : 1;
        }
        else if (row.type === 'del') {
            count -= row.exists ? 1 : 0;
        }
        cb(null, { 'whatever': row.value });
    });
    
    db.batch([
        { type: 'put', key: 'a', value: 123 },
        { type: 'put', key: 'b', value: 555 },
        { type: 'put', key: 'a', value: 444 },
        { type: 'put', key: 'c', value: 1000 },
        { type: 'del', key: 'b' }
    ], ready);
    
    indexes.on('change', function (ch) {
        if (ch.change === 1) {
            t.equal(count, 2);
        }
    });
    
    function ready (err) {
        t.ifError(err);
    }
    function relevant (x) {
        if (x.type === 'del') {
            return { type: x.type, key: x.key, exists: x.exists };
        }
        return { type: x.type, key: x.key, value: x.value, exists: x.exists };
    }
});
