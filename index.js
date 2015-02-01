var chproc = require('level-change-processor');
var sublevel = require('subleveldown');
var isarray = require('isarray');
var inherits = require('inherits');
var EventEmitter = require('events').EventEmitter;
var changesdown = require('changesdown');
var xtend = require('xtend');

module.exports = Ix;
inherits(Ix, EventEmitter);

function Ix (db, feed) {
    if (!(this instanceof Ix)) return new Ix(db, feed);
    EventEmitter.call(this);
    this.db = db;
    this.feed = feed;
    this.names = {};
}

Ix.prototype.add = function (name, fn) {
    var self = this;
    var rdb = this._getName(name).result;
    var cdb = this._getName(name).change;
    var proc = chproc({ db: cdb, feed: this.feed, worker: worker });
    this.names[name] = { change: cdb, result: rdb };
    
    proc.on('error', function (err) {
        self.emit('error', err);
    });
    
    function worker (ch, cb) {
        fn(self._decode(ch.value), function (err, rows) {
            if (err) return cb(err);
            if (!rows || rows.length === 0) return cb();
            if (!isarray(rows)) rows = [ rows ];
            rdb.batch(rows, cb);
        });
    }
};

Ix.prototype.clear = function (name, cb) {
    var rdb = this._getName(name).result;
    var cdb = this._getName(name).change;
    var ops = [];
    var pending = 2;
    
    rdb.createReadStream().pipe(through.obj(write, end));
    cdb.createReadStream().pipe(through.obj(write, end));
    
    function write (row, enc, next) {
        ops.push(row.key);
        next();
    }
    function end () {
        if (-- pending === 0) db.batch(ops, cb);
    }
};

Ix.prototype._getName = function (name) {
    if (!this.names[name]) {
        var rdb = sublevel(this.db, 'r!' + name);
        var cdb = sublevel(this.db, 'c!' + name);
        this.names[name] = { result: rdb, change: cdb };
    }
    return this.names[name];
};

Ix.prototype._decode = function (x) {
    var d = changesdown.decode(x);
    var batch = d.type === 'batch' ? d.batch : [ d ];
    var codec = xtend({
        decodeKey: function (x) { return x },
        decodeValue: function (x) { return x }
    }, this.db._codec);
    var options = this.db.options;
    
    return batch.map(function (b) {
        return {
            type: b.type,
            key: unbuf(codec.decodeKey(b.key, options)),
            value: codec.decodeValue(b.value, options)
        };
    });
};

function unbuf (buf) {
    if (Buffer.isBuffer(buf)) return buf.toString('utf8');
    return buf;
}
