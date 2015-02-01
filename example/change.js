var level = require('level');
var sublevel = require('subleveldown');
var changes = require('changes-feed');
var changesdown = require('changesdown');
var Ix = require('../');

var up = level('test.db', { valueEncoding: 'json' });
var feed = changes(sublevel(up, 'feed'));
var db = changesdown(sublevel(up, 'db'), feed, { valueEncoding: 'json' });

var indexes = Ix(sublevel(up, 'ix'), feed);
indexes.add('user-name', function (updates, batch) {
    batch(null, updates);
});

var rows = [
    { type: 'put', key: 'abc123', value: { type: 'user', name: 'substack' } }
];
db.batch(rows, function (err) {
    console.log('err=', err);
});
