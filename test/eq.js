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

test('eq', function (t) {
    t.plan(3);
    var up = level(name('main.db'), { valueEncoding: 'json' });
    var feed = changes(sublevel(up, 'feed'));
    var db = changesdown(sublevel(up, 'db'), feed, { valueEncoding: 'json' });
    
    var indexes = chi({
        ixdb: level(name('index.db'), { valueEncoding: 'json' }),
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
        indexes.createReadStream('user.name', { eq: 'mitch' })
        .pipe(collect(function (rows) {
            t.deepEqual(rows, [
                {
                    key: 'user!3',
                    value: { name: 'mitch', hackerspace: 'noisebridge' },
                    index: 'mitch'
                }
            ], 'user.name eq mitch');
        }));
        indexes.createReadStream('user.space', { eq: 'sudoroom' })
        .pipe(collect(function (rows) {
            t.deepEqual(rows, [
                {
                    key: 'user!1',
                    value: { name: 'substack', hackerspace: 'sudoroom' },
                    index: 'sudoroom'
                },
                {
                    key: 'user!2',
                    value: { name: 'mk30', hackerspace: 'sudoroom' },
                    index: 'sudoroom'
                },
            ], 'user.space eq sudoroom');
        }));
    }
});

function collect (cb) {
    var rows = [];
    return through.obj(write, end);
    function write (row, enc, next) { rows.push(row); next() }
    function end () { cb(rows) }
}
