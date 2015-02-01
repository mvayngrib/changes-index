var level = require('level-test')();
var sublevel = require('subleveldown');
var through = require('through2');
var changes = require('changes-feed');
var changesdown = require('changesdown');
var chindexer = require('../');
var test = require('tape');

test('basic indexing', function (t) {
    t.plan(9);
    var up = level('main.db', { valueEncoding: 'json' });
    var feed = changes(sublevel(up, 'feed'));
    var db = changesdown(sublevel(up, 'db'), feed, { valueEncoding: 'json' });
    
    var indexes = chindexer({
        ixdb: level('index.db', { valueEncoding: 'json' }),
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
    
    db.batch([
        {
            type: 'put',
            key: 'user!1',
            value: { name: 'substack', hackerspace: 'sudoroom' }
        },
        {
            type: 'put',
            key: 'user!2',
            value: { name: 'mk30', hackerspace: 'sudoroom' }
        },
        {
            type: 'put',
            key: 'user!3',
            value: { name: 'mitch', hackerspace: 'noisebridge' }
        },
        {
            type: 'put',
            key: 'ignore!me',
            value: {}
        }
    ], ready);
    
    function ready (err) {
        t.ifError(err);
        indexes.createReadStream('user.name')
        .pipe(collect(function (rows) {
            t.deepEqual(rows, [
                {
                    key: 'user!3',
                    value: { name: 'mitch', hackerspace: 'noisebridge' },
                    index: 'mitch'
                },
                {
                    key: 'user!2',
                    value: { name: 'mk30', hackerspace: 'sudoroom' },
                    index: 'mk30'
                },
                {
                    key: 'user!1',
                    value: { name: 'substack', hackerspace: 'sudoroom' },
                    index: 'substack'
                }
            ], 'user.name all');
        }));
        indexes.createReadStream('user.name', { lt: 'mk30' })
        .pipe(collect(function (rows) {
            t.deepEqual(rows, [
                {
                    key: 'user!3',
                    value: { name: 'mitch', hackerspace: 'noisebridge' },
                    index: 'mitch'
                }
            ], 'user.name lte');
        }));
        indexes.createReadStream('user.name', { lte: 'mk30' })
        .pipe(collect(function (rows) {
            t.deepEqual(rows, [
                {
                    key: 'user!3',
                    value: { name: 'mitch', hackerspace: 'noisebridge' },
                    index: 'mitch'
                },
                {
                    key: 'user!2',
                    value: { name: 'mk30', hackerspace: 'sudoroom' },
                    index: 'mk30'
                }
            ], 'user.name lte');
        }));
        indexes.createReadStream('user.name', { gt: 'mk30' })
        .pipe(collect(function (rows) {
            t.deepEqual(rows, [
                {
                    key: 'user!1',
                    value: { name: 'substack', hackerspace: 'sudoroom' },
                    index: 'substack'
                }
            ], 'user.name gt');
        }));
        indexes.createReadStream('user.name', { gte: 'mk30' })
        .pipe(collect(function (rows) {
            t.deepEqual(rows, [
                {
                    key: 'user!2',
                    value: { name: 'mk30', hackerspace: 'sudoroom' },
                    index: 'mk30'
                },
                {
                    key: 'user!1',
                    value: { name: 'substack', hackerspace: 'sudoroom' },
                    index: 'substack'
                }
            ], 'user.name gt');
        }));
        indexes.createReadStream('user.space')
        .pipe(collect(function (rows) {
            t.deepEqual(rows, [
                {
                    key: 'user!3',
                    value: { name: 'mitch', hackerspace: 'noisebridge' },
                    index: 'noisebridge'
                },
                {
                    key: 'user!1',
                    value: { name: 'substack', hackerspace: 'sudoroom' },
                    index: 'sudoroom'
                },
                {
                    key: 'user!2',
                    value: { name: 'mk30', hackerspace: 'sudoroom' },
                    index: 'sudoroom'
                }
            ], 'user.space all');
        }));
        indexes.createReadStream('user.space', { lt: 'sudoroom' })
        .pipe(collect(function (rows) {
            t.deepEqual(rows, [
                {
                    key: 'user!3',
                    value: { name: 'mitch', hackerspace: 'noisebridge' },
                    index: 'noisebridge'
                }
            ], 'user.space lt sudoroom');
        }));
        indexes.createReadStream('user.space', { limit: 2 })
        .pipe(collect(function (rows) {
            t.deepEqual(rows, [
                {
                    key: 'user!3',
                    value: { name: 'mitch', hackerspace: 'noisebridge' },
                    index: 'noisebridge'
                },
                {
                    key: 'user!1',
                    value: { name: 'substack', hackerspace: 'sudoroom' },
                    index: 'sudoroom'
                }
            ], 'user.space limit 2');
        }));
    }
});

function collect (cb) {
    var rows = [];
    return through.obj(write, end);
    function write (row, enc, next) { rows.push(row); next() }
    function end () { cb(rows) }
}
