var level = require('level');
var sublevel = require('subleveldown');
var changes = require('changes-feed');
var changesdown = require('changesdown');
var Ix = require('../');

var up = level('test.db', { valueEncoding: 'json' });
var feed = changes(sublevel(up, 'feed'));
var db = changesdown(sublevel(up, 'db'), feed, { valueEncoding: 'json' });

var indexes = Ix(sublevel(up, 'ix'), feed);
indexes.add('user-name', function (ch, batch) {
    console.log(decode(ch.value));
    batch(null, []);
});

db.batch([
    { type: 'put', key: 'abc123', value: { type: 'user', name: 'substack' } }
], ready);

function ready () {
}

function decode (x) {
    var d = changesdown.decode(x);
    var batch = d.type === 'batch' ? d.batch : [ d ];
    
    return batch.map(function (b) {
        return {
            type: b.type,
            key: unbuf(db._codec.decodeKey(b.key, db.options)),
            value: db._codec.decodeValue(b.value, db.options)
        };
    });
}
function unbuf (buf) {
    if (Buffer.isBuffer(buf)) return buf.toString('utf8');
    return buf;
}
