var level = require('level-test')();
var sublevel = require('subleveldown');
var through = require('through2');
var changes = require('changes-feed');
var changesdown = require('changesdown');
var chindexer = require('../');
var test = require('tape');

function name (x) {
    return x + Math.floor(Math.pow(16,8)*Math.random()).toString(16);
}

test('update existing indexes', function (t) {
    t.plan(4);
    var up = level(name('main.db'), { valueEncoding: 'json' });
    var feed = changes(sublevel(up, 'feed'));
    var db = changesdown(sublevel(up, 'db'), feed, { valueEncoding: 'json' });
    
    var indexes = chindexer({
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
    
    indexes.once('change', ready);
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
    ], function (err) { t.ifError(err) });
    
    function ready () {
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
            
            indexes.once('change', checkNameUpdate);
            db.put('user!1',
                { name: 'scubastack', hackerspace: 'sudoroom' },
                function (err) { t.ifError(err) }
            );
        }));
    }
    
    function checkNameUpdate () {
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
                    value: { name: 'scubastack', hackerspace: 'sudoroom' },
                    index: 'scubastack'
                }
            ], 'user.name all');
        }));
    }
});

function collect (cb) {
    var rows = [];
    return through.obj(write, end);
    function write (row, enc, next) { rows.push(row); next() }
    function end () { cb(rows) }
}
